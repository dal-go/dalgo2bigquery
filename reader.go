package bigquery

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"sync"
)

type cursorState struct {
	Version      int       `json:"version"`
	RunID        string    `json:"runId"`
	Job          JobRef    `json:"job"`
	PlanDigest   string    `json:"planDigest"`
	SchemaDigest string    `json:"schemaDigest"`
	Principal    Principal `json:"principal"`
	PageToken    *string   `json:"pageToken"`
	PageSize     int       `json:"pageSize"`
	Offset       int       `json:"offset"`
	PageDigest   string    `json:"pageDigest"`
	Ordinal      int       `json:"ordinal"`
	Counters     Counters  `json:"counters"`
	LedgerRef    string    `json:"ledgerRef"`
}
type Run struct {
	c              *Client
	ctx            context.Context
	cancel         context.CancelFunc
	id             string
	mu             sync.Mutex
	rows           [][]Cell
	rs             recordset.Recordset
	index          int
	loaded, done   bool
	mode           string
	emptyDelivered bool
	access         Principal
}

func newRun(c *Client, ctx context.Context, id string) *Run {
	ctx, cancel := context.WithCancel(ctx)
	rr, _ := c.loadRun(id)
	return &Run{c: c, ctx: ctx, cancel: cancel, id: id, access: rr.ActivePrincipal}
}
func (r *Run) Receipt() Receipt {
	record, e := r.c.loadRun(r.id)
	if e != nil {
		return Receipt{RunID: r.id, Reason: codeOf(e)}
	}
	return record.Receipt
}
func (r *Run) Schema() []Field {
	record, e := r.c.loadRun(r.id)
	if e != nil {
		return nil
	}
	fs, _ := jsonCopy(record.Schema)
	return fs
}
func (r *Run) Recordset() recordset.Recordset { r.mu.Lock(); defer r.mu.Unlock(); return r.rs }
func (r *Run) Close() error {
	r.cancel()
	return r.c.mutateRun(r.id, func(rr *runRecord) error {
		rr.Receipt.LocalStopped = true
		rr.Receipt.Reason = "local_stopped"
		return nil
	})
}
func (r *Run) stopped(e error) {
	_ = r.c.mutateRun(r.id, func(rr *runRecord) error {
		rr.Receipt.LocalStopped = true
		rr.Receipt.Reason = reasonOf(e)
		return nil
	})
}
func (r *Run) loadPage(m map[string]any, token *string, refetch bool) error {
	record, e := r.c.loadRun(r.id)
	if e != nil {
		return e
	}
	if record.Receipt.Job == nil {
		return fail("malformed_wire")
	}
	if e = known(m, []string{"kind", "etag", "jobReference", "jobComplete", "schema", "rows", "pageToken", "totalRows", "totalBytesProcessed", "totalBytesBilled", "cacheHit", "errors", "jobCreationReason", "creationTime", "startTime", "endTime", "location", "queryId", "statementType", "pageRowCount", "totalSlotMs"}); e != nil {
		return fail("malformed_wire")
	}
	if j, ok := m["jobReference"]; ok {
		job, e := parseJob(j)
		if e != nil || job != *record.Receipt.Job {
			return fail("malformed_wire")
		}
	}
	complete, ok := m["jobComplete"].(bool)
	if !ok {
		return fail("malformed_wire")
	}
	fs := record.Schema
	if v, ok := m["schema"]; ok {
		incoming, e := decodeSchema(v)
		if e != nil {
			return e
		}
		expected := []Field{}
		profile := r.c.profiles[record.Preview.Plan.SourceDigest]
		for _, n := range record.Preview.Plan.Projection {
			for _, f := range profile.Schema {
				if f.Name == n {
					expected = append(expected, f)
				}
			}
		}
		if !equalJSON(expected, incoming) || len(fs) > 0 && !equalJSON(fs, incoming) {
			return fail("schema_changed")
		}
		fs = incoming
	}
	var rows [][]Cell
	rawRows := m["rows"]
	if rawRows == nil {
		if _, present := m["rows"]; present {
			return fail("malformed_wire")
		}
		rawRows = []any{}
	}
	if len(fs) == 0 {
		if a, ok := rawRows.([]any); !ok || len(a) > 0 || complete {
			return fail("malformed_wire")
		}
		r.rows = nil
		r.loaded = false
		r.done = false
		return nil
	}
	bytes, e := json.Marshal(rawRows)
	if e != nil {
		return fail("malformed_wire")
	}
	rows, e = DecodeRows(bytes, fs, record.Receipt.Bounds.ResponseBytes)
	if e != nil {
		return e
	}
	if len(rows) > record.Receipt.Bounds.PageSize {
		return fail("response_limit")
	}
	var next *string
	if v, ok := m["pageToken"]; ok {
		s, ok := v.(string)
		if !ok || s == "" || len(s) > 16384 {
			return fail("malformed_wire")
		}
		next = &s
	}
	canonicalRows, e := CanonicalJSON(bytes)
	if e != nil {
		return e
	}
	digest := hashBytes(canonicalRows)
	if refetch && digest != record.PageDigest {
		return fail("cursor_invalid")
	}
	if refetch && (!equalJSON(next, record.NextToken) || complete != record.PageDone) {
		return fail("cursor_invalid")
	}
	processed, e := decimalString(m, "totalBytesProcessed")
	if e != nil {
		return e
	}
	billed, e := decimalString(m, "totalBytesBilled")
	if e != nil {
		return e
	}
	var cache *bool
	if v, ok := m["cacheHit"]; ok {
		b, ok := v.(bool)
		if !ok {
			return fail("malformed_wire")
		}
		cache = &b
	}
	e = r.c.mutateRun(r.id, func(rr *runRecord) error {

		if !refetch {
			if next != nil && (rr.SeenTokens[*next] || token != nil && *next == *token) {
				return fail("cursor_invalid")
			}
			if token != nil {
				rr.SeenTokens[*token] = true
			}
			rr.PageOrdinal++
			rr.Offset = 0
			rr.PageToken = token
			rr.NextToken = next
			rr.PageDigest = digest
			rr.PageDone = complete
		}
		rr.Schema = fs
		rr.Receipt.SchemaDigest = schemaDigest(fs)
		rr.Receipt.ProcessedBytes = processed
		rr.Receipt.BilledBytes = billed
		rr.Receipt.CacheHit = cache
		rr.Receipt.Warnings = append(rr.Receipt.Warnings, providerWarnings(m["errors"])...)
		return nil
	})
	if e != nil {
		return e
	}
	record, e = r.c.loadRun(r.id)
	if e != nil {
		return e
	}
	if record.Receipt.Counters.Pages > record.Receipt.Bounds.MaxPages {
		return fail("response_limit")
	}
	r.rows = rows
	r.index = record.Offset
	if r.index > len(rows) {
		return fail("cursor_invalid")
	}
	r.loaded = true
	r.done = complete && next == nil
	cols := []recordset.Column[any]{}
	for _, f := range fs {
		cols = append(cols, recordset.NewTypedColumn[any](f.Name, nil, recordset.ColDbType(f.Type)))
	}
	rs := recordset.NewColumnarRecordset("bigquery", cols...)
	for _, cells := range rows {
		row := rs.NewRow()
		for i, cell := range cells {
			if e := row.SetValueByIndex(i, cell.Value, rs); e != nil {
				return fail("malformed_wire")
			}
		}
	}
	r.rs = rs
	return nil
}
func (r *Run) fetch(token *string, refetch bool) error {
	rr, e := r.c.loadRun(r.id)
	if e != nil {
		return e
	}
	scope := runScope(rr)
	bounded, cancel, e := r.c.preparation(r.ctx, scope, false)
	if e != nil {
		return e
	}
	defer cancel()
	if e = r.c.reauthorize(bounded, rr.Preview.Plan, rr.Preview.PolicyDigest); e != nil {
		return e
	}
	if rr.Receipt.Job == nil {
		return fail("submission_unknown")
	}
	job := *rr.Receipt.Job
	scope.result = true
	raw, e := r.c.call(bounded, scope, &rr.ActivePrincipal, false, func(api sdkAPI) error {
		call := api.service.Jobs.GetQueryResults(job.ProjectID, job.JobID).Location(job.Location).MaxResults(int64(rr.Receipt.Bounds.PageSize)).FormatOptionsUseInt64Timestamp(true).TimeoutMs(1000)
		if token == nil {
			call = call.StartIndex(0)
		} else {
			call = call.PageToken(*token)
		}
		_, e := call.Context(api.ctx).Do()
		return e
	})
	if e != nil {
		return e
	}
	m, e := object(raw)
	if e != nil {
		return e
	}
	return r.loadPage(m, token, refetch)
}
func (r *Run) ensure() error {
	for {
		rr, e := r.c.loadRun(r.id)
		if e != nil {
			return e
		}
		if r.ctx.Err() != nil {
			return fail("local_stopped")
		}
		if !r.c.clock.Now().Before(rr.Receipt.ExecutionDeadline) {
			return deadlineError()
		}
		if r.access != rr.ActivePrincipal {
			return fail("approval_changed")
		}
		if r.loaded && r.index == len(r.rows) && r.done && (len(r.rows) > 0 || r.emptyDelivered) {
			return dal.ErrNoMoreRecords
		}
		if rr.Receipt.Counters.Rows >= rr.Receipt.Bounds.MaxRows {
			return fail("response_limit")
		}
		bounded, cancel, e := r.c.preparation(r.ctx, runScope(rr), false)
		if e != nil {
			return e
		}
		e = r.c.reauthorize(bounded, rr.Preview.Plan, rr.Preview.PolicyDigest)
		if e == nil {
			e = r.c.checkIdentity(bounded, rr.ActivePrincipal)
		}
		cancel()
		if e != nil {
			return e
		}
		if !r.c.clock.Now().Before(rr.Receipt.ExecutionDeadline) {
			return deadlineError()
		}
		if r.loaded && r.index != rr.Offset {
			return fail("cursor_invalid")
		}
		if r.loaded && r.index < len(r.rows) {
			return nil
		}
		if r.loaded && r.done {
			return dal.ErrNoMoreRecords
		}
		token := rr.NextToken
		if !r.loaded {
			token = rr.PageToken
		}
		if e = r.fetch(token, false); e != nil {
			return e
		}
		if !r.loaded {
			if e = r.c.clock.Sleep(r.ctx, 100000000); e != nil {
				return e
			}
		}
	}
}
func (r *Run) commitDelivery(n int) (string, error) {
	var cursor cursorState
	e := r.c.mutateRun(r.id, func(rr *runRecord) error {
		if n < 0 || n > rr.Receipt.Bounds.MaxRows-rr.Receipt.Counters.Rows {
			return fail("response_limit")
		}
		rr.Offset = r.index + n
		rr.Receipt.Counters.Rows += n
		token := rr.PageToken
		offset := rr.Offset
		pageDigest := rr.PageDigest
		ordinal := rr.PageOrdinal
		if rr.Offset == len(r.rows) && rr.NextToken != nil {
			token = rr.NextToken
			offset = 0
			pageDigest = ""
			ordinal++
		}
		cursor = cursorState{1, r.id, *rr.Receipt.Job, rr.Preview.Plan.Digest, rr.Receipt.SchemaDigest, rr.ActivePrincipal, token, rr.Receipt.Bounds.PageSize, offset, pageDigest, ordinal, rr.Receipt.Counters, opaqueID()}
		rr.Cursor = &cursor
		return nil
	})
	if e != nil {
		return "", e
	}
	r.index += n
	raw, _ := json.Marshal(cursor)
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
func (r *Run) Cursor() (string, error) {
	rr, e := r.c.loadRun(r.id)
	if e != nil {
		return "", e
	}
	if rr.Cursor == nil {
		return "", fail("cursor_invalid")
	}
	raw, _ := json.Marshal(rr.Cursor)
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
func (r *Run) Next() (recordset.Row, recordset.Recordset, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mode == "page" {
		return nil, nil, fail("invalid_input")
	}
	r.mode = "row"
	release, e := r.c.ledger.lease(r.ctx, r.id)
	if e != nil {
		return nil, nil, e
	}
	defer release()
	if e = r.ensure(); e != nil {
		if e != dal.ErrNoMoreRecords {
			r.stopped(e)
		}
		return nil, nil, e
	}
	row := r.rs.GetRow(r.index)
	if _, e = r.commitDelivery(1); e != nil {
		return nil, nil, e
	}
	return row, r.rs, nil
}
func (r *Run) NextPage() (Page, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mode == "row" {
		return Page{}, fail("invalid_input")
	}
	r.mode = "page"
	release, e := r.c.ledger.lease(r.ctx, r.id)
	if e != nil {
		return Page{}, e
	}
	defer release()
	if e = r.ensure(); e != nil {
		if e == dal.ErrNoMoreRecords && r.loaded && len(r.rows) == 0 && !r.emptyDelivered {
			r.emptyDelivered = true
			cursor, e := r.commitDelivery(0)
			return Page{r.Schema(), [][]Cell{}, r.Receipt(), cursor}, e
		}
		if e != dal.ErrNoMoreRecords {
			r.stopped(e)
		}
		return Page{}, e
	}
	rr, e := r.c.loadRun(r.id)
	if e != nil {
		return Page{}, e
	}
	n := len(r.rows) - r.index
	if rem := rr.Receipt.Bounds.MaxRows - rr.Receipt.Counters.Rows; n > rem {
		n = rem
	}
	rows, _ := jsonCopy(r.rows[r.index : r.index+n])
	cursor, e := r.commitDelivery(n)
	if e != nil {
		return Page{}, e
	}
	return Page{r.Schema(), rows, r.Receipt(), cursor}, nil
}
func (c *Client) Resume(ctx context.Context, receipt Receipt, encoded string) (*Run, error) {
	raw, e := base64.RawURLEncoding.DecodeString(encoded)
	if e != nil || len(raw) > 32768 {
		return nil, fail("cursor_invalid")
	}
	if _, e = ParseJSON(raw, 32768); e != nil {
		return nil, fail("cursor_invalid")
	}
	var cursor cursorState
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if e = dec.Decode(&cursor); e != nil {
		return nil, fail("cursor_invalid")
	}
	rr, e := c.loadRun(receipt.RunID)
	if e != nil {
		return nil, e
	}
	if cursor.RunID != receipt.RunID || rr.Cursor == nil || !equalJSON(*rr.Cursor, cursor) || !receiptMatches(receipt, rr.Receipt) {
		return nil, fail("cursor_invalid")
	}
	run := newRun(c, ctx, receipt.RunID)
	if !c.clock.Now().Before(rr.Receipt.ExecutionDeadline) {
		run.stopped(deadlineError())
		return run, deadlineError()
	}
	release, e := c.ledger.lease(ctx, run.id)
	if e != nil {
		return run, e
	}
	defer release()
	rr, e = c.loadRun(run.id)
	if e != nil {
		return run, e
	}
	if rr.Cursor == nil || !equalJSON(*rr.Cursor, cursor) {
		return run, fail("cursor_invalid")
	}
	if e = c.reauthorize(ctx, rr.Preview.Plan, rr.Preview.PolicyDigest); e != nil {
		return run, e
	}
	// Only the trusted cursor advances the page boundary. A partial cursor must
	// refetch the same page and validate its content before skipping its offset.
	_ = c.mutateRun(run.id, func(r *runRecord) error { r.Receipt.LocalStopped = false; r.Receipt.Reason = ""; return nil })
	refetch := cursor.PageDigest != ""
	if !refetch {
		e = c.mutateRun(run.id, func(r *runRecord) error { r.PageToken = cursor.PageToken; r.Offset = 0; return nil })
		if e != nil {
			return run, e
		}
	}
	if e = run.fetch(cursor.PageToken, refetch); e != nil {
		run.stopped(e)
		return run, e
	}
	return run, nil
}
func (e Executor) ExecuteQueryToRecordsetReader(ctx context.Context, q dal.Query, options ...recordset.Option) (dal.RecordsetReader, error) {
	plan, err := Compile(e.Profile, q)
	if err != nil {
		return nil, err
	}
	if plan.Digest != e.Plan.Digest {
		return nil, fail("approval_changed")
	}
	return e.Client.Execute(ctx, e.Approval)
}

var _ dal.RecordsetReader = (*Run)(nil)
var _ dal.QueryExecutor = Executor{}

func receiptMatches(a, b Receipt) bool {
	return a.Version == b.Version && a.RunID == b.RunID && a.ApprovalDigest == b.ApprovalDigest && a.SourceDigest == b.SourceDigest && a.ObservationDigest == b.ObservationDigest && a.SchemaDigest == b.SchemaDigest && a.Principal == b.Principal && equalJSON(a.Job, b.Job) && equalJSON(a.Bounds, b.Bounds) && a.RunStartedAt.Equal(b.RunStartedAt) && a.ExecutionDeadline.Equal(b.ExecutionDeadline)
}
