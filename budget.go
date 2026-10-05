package bigquery

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"
)

var regexpUnsigned = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

func positiveBytes(s string) (int64, error) {
	if !regexpUnsigned.MatchString(s) {
		return 0, fail("invalid_input")
	}
	n, e := strconv.ParseInt(s, 10, 64)
	if e != nil || n < 1 {
		return 0, fail("invalid_input")
	}
	return n, nil
}
func opaqueID() string {
	b := make([]byte, 24)
	if _, e := rand.Read(b); e != nil {
		panic("secure random unavailable")
	}
	return hex.EncodeToString(b)
}

type previewRecord struct {
	Preview Preview `json:"preview"`
	Used    bool    `json:"used"`
}
type runRecord struct {
	ActivePrincipal Principal       `json:"activePrincipal"`
	Receipt         Receipt         `json:"receipt"`
	Preview         Preview         `json:"preview"`
	Reservation     int64           `json:"reservation"`
	PageToken       *string         `json:"pageToken"`
	NextToken       *string         `json:"nextToken"`
	PageDigest      string          `json:"pageDigest"`
	Offset          int             `json:"offset"`
	PageOrdinal     int             `json:"pageOrdinal"`
	PageDone        bool            `json:"pageDone"`
	Schema          []Field         `json:"schema"`
	SeenTokens      map[string]bool `json:"seenTokens"`
	Cursor          *cursorState    `json:"cursor"`
}
type ledgerState struct {
	Version  int                      `json:"version"`
	Previews map[string]previewRecord `json:"previews"`
	Runs     map[string]runRecord     `json:"runs"`
}

func emptyLedger() ledgerState {
	return ledgerState{1, map[string]previewRecord{}, map[string]runRecord{}}
}

// Ledger serializes durable updates and separately holds per-run leases during
// network and delivery operations. File leases are released by the OS on exit,
// so deliberate Resume retains state while recovering after process termination.
// Applications should reuse one private FileLedger path for the budget session.
type Ledger interface {
	update(func(*ledgerState) error) error
	updateContext(context.Context, func(*ledgerState) error) error
	readContext(context.Context, func(*ledgerState) error) error
	lease(context.Context, string) (func(), error)
}
type MemoryLedger struct {
	mu     sync.Mutex
	state  ledgerState
	leases sync.Map
}

func NewMemoryLedger() *MemoryLedger { return &MemoryLedger{state: emptyLedger()} }
func (m *MemoryLedger) update(fn func(*ledgerState) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return m.updateContext(ctx, fn)
}
func (m *MemoryLedger) updateContext(ctx context.Context, fn func(*ledgerState) error) error {
	for !m.mu.TryLock() {
		select {
		case <-ctx.Done():
			return deadlineError()
		case <-time.After(time.Millisecond):
		}
	}
	if ctx.Err() != nil {
		m.mu.Unlock()
		return deadlineError()
	}
	defer m.mu.Unlock()
	next, e := jsonCopy(m.state)
	if e != nil {
		return e
	}
	if e = fn(&next); e != nil {
		return e
	}
	stored, e := jsonCopy(next)
	if e != nil {
		return e
	}
	if ctx.Err() != nil {
		return deadlineError()
	}
	m.state = stored
	return nil
}
func (m *MemoryLedger) lease(ctx context.Context, id string) (func(), error) {
	if e := ctx.Err(); e != nil {
		return nil, fail("local_stopped")
	}
	mu, _ := m.leases.LoadOrStore(id, &sync.Mutex{})
	if !mu.(*sync.Mutex).TryLock() {
		return nil, fail("local_stopped")
	}
	return mu.(*sync.Mutex).Unlock, nil
}

type FileLedger struct{ dir string }

func NewFileLedger(dir string) (*FileLedger, error) {
	if !filepath.IsAbs(dir) {
		return nil, fail("invalid_input")
	}
	if e := ledgerMkdir(dir); e != nil {
		return nil, fail("invalid_input")
	}
	s, e := os.Lstat(dir)
	if e != nil || !s.IsDir() || !ledgerPrivatePath(dir, true) {
		return nil, fail("invalid_input")
	}
	return &FileLedger{dir}, nil
}
func (l *FileLedger) lock(name string) (*os.File, error) {
	f, e := ledgerOpen(filepath.Join(l.dir, name), true)
	if e != nil {
		return nil, fail("invalid_input")
	}
	s, e := f.Stat()
	if e != nil || !s.Mode().IsRegular() || !ledgerPrivateFile(f) {
		f.Close()
		return nil, fail("invalid_input")
	}
	return f, nil
}
func (l *FileLedger) lease(ctx context.Context, id string) (func(), error) {
	if !regexp.MustCompile(`^[0-9a-f]{48}$`).MatchString(id) {
		return nil, fail("invalid_input")
	}
	if ctx.Err() != nil {
		return nil, fail("local_stopped")
	}
	f, e := l.lock("run-" + id + ".lock")
	if e != nil {
		return nil, e
	}
	if e = ledgerLock(f, true); e != nil {
		f.Close()
		return nil, fail("local_stopped")
	}
	return func() { _ = ledgerUnlock(f); _ = f.Close() }, nil
}
func (l *FileLedger) update(fn func(*ledgerState) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return l.updateContext(ctx, fn)
}
func (l *FileLedger) updateContext(ctx context.Context, fn func(*ledgerState) error) error {
	lock, e := l.lock("session.lock")
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = ledgerLockContext(ctx, lock); e != nil {
		return e
	}
	defer ledgerUnlock(lock)
	state, e := l.readState(ctx)
	if e != nil {
		return e
	}
	if ctx.Err() != nil {
		return deadlineError()
	}
	if e = fn(&state); e != nil {
		return e
	}
	data, e := json.Marshal(state)
	if e != nil || len(data) > 64<<20 {
		return fail("response_limit")
	}
	temp, e := ledgerTemp(l.dir)
	if e != nil {
		return fail("invalid_input")
	}
	name := temp.Name()
	defer os.Remove(name)
	if e = ledgerSecureTemp(temp); e == nil {
		_, e = temp.Write(data)
	}
	if e == nil {
		e = temp.Sync()
	}
	ce := temp.Close()
	if e != nil || ce != nil {
		return fail("invalid_input")
	}
	if ctx.Err() != nil {
		return deadlineError()
	}
	if e = ledgerReplace(name, filepath.Join(l.dir, "session.json")); e != nil {
		return fail("invalid_input")
	}
	return ledgerSyncDir(l.dir)
}
func (c *Client) loadRun(id string) (runRecord, error) {
	var run runRecord
	e := c.ledger.readContext(context.Background(), func(s *ledgerState) error {
		r, ok := s.Runs[id]
		if !ok {
			return fail("cursor_invalid")
		}
		run = r
		return nil
	})
	return run, e
}
func (c *Client) mutateRun(id string, fn func(*runRecord) error) error {
	return c.ledger.update(func(s *ledgerState) error {
		r, ok := s.Runs[id]
		if !ok {
			return fail("cursor_invalid")
		}
		if e := fn(&r); e != nil {
			return e
		}
		s.Runs[id] = r
		return nil
	})
}
func budgetKey(p Preview) string {
	return p.Execution.Principal.Kind + "\x00" + p.Execution.Principal.Subject + "\x00" + p.Execution.JobProject
}

func (l *FileLedger) readState(ctx context.Context) (ledgerState, error) {
	state := emptyLedger()
	path := filepath.Join(l.dir, "session.json")
	f, e := ledgerOpen(path, false)
	if e == nil {
		s, se := f.Stat()
		if se != nil || !s.Mode().IsRegular() || !ledgerPrivateFile(f) || s.Size() > 64<<20 {
			f.Close()
			return state, fail("invalid_input")
		}
		dec := json.NewDecoder(f)
		dec.DisallowUnknownFields()
		e = dec.Decode(&state)
		f.Close()
		if e != nil || state.Version != 1 || state.Runs == nil || state.Previews == nil {
			return state, fail("invalid_input")
		}
	} else if !os.IsNotExist(e) {
		return state, fail("invalid_input")
	}
	if ctx.Err() != nil {
		return state, deadlineError()
	}
	return state, nil
}

func (m *MemoryLedger) readContext(ctx context.Context, fn func(*ledgerState) error) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return m.updateContext(ctx, func(s *ledgerState) error {
		copy, e := jsonCopy(*s)
		if e != nil {
			return e
		}
		return fn(&copy)
	})
}
func (l *FileLedger) readContext(ctx context.Context, fn func(*ledgerState) error) error {
	s, e := l.readState(ctx)
	if e != nil {
		return e
	}
	return fn(&s)
}
func ledgerLockContext(ctx context.Context, f *os.File) error {
	for {
		if ctx.Err() != nil {
			return deadlineError()
		}
		if e := ledgerLock(f, true); e == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return deadlineError()
		case <-time.After(time.Millisecond):
		}
	}
}
func (c *Client) mutateRunContext(ctx context.Context, id string, fn func(*runRecord) error) error {
	return c.ledger.updateContext(ctx, func(s *ledgerState) error {
		r, ok := s.Runs[id]
		if !ok {
			return fail("cursor_invalid")
		}
		if ctx.Err() != nil {
			return deadlineError()
		}
		if e := fn(&r); e != nil {
			return e
		}
		s.Runs[id] = r
		return nil
	})
}
