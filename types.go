package bigquery

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

type SourceProfile struct {
	Version            int     `json:"version"`
	SourceID           string  `json:"sourceId"`
	DescriptorDigest   string  `json:"descriptorDigest"`
	LogicalCollection  string  `json:"logicalCollection"`
	SourceProject      string  `json:"sourceProject"`
	DatasetID          string  `json:"datasetId"`
	TableID            string  `json:"tableId"`
	Location           string  `json:"location"`
	Schema             []Field `json:"schema"`
	PublisherReviewRef string  `json:"publisherReviewRef"`
	RightsReviewRef    string  `json:"rightsReviewRef"`
	Use                string  `json:"use"`
}
type Predicate struct {
	Op        string      `json:"op"`
	Column    string      `json:"column,omitempty"`
	Parameter string      `json:"parameter,omitempty"`
	Children  []Predicate `json:"children,omitempty"`
}
type Order struct {
	Column    string `json:"column"`
	Direction string `json:"direction"`
}
type Parameter struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}
type ReadPlan struct {
	Version      int         `json:"version"`
	SourceDigest string      `json:"sourceDigest"`
	Projection   []string    `json:"projection"`
	Where        *Predicate  `json:"where"`
	Order        []Order     `json:"order"`
	Limit        int         `json:"limit"`
	Parameters   []Parameter `json:"parameters"`
	SQL          string      `json:"sql"`
	Digest       string      `json:"digest"`
}
type Principal struct {
	Kind       string `json:"kind"`
	Subject    string `json:"subject"`
	Generation string `json:"generation"`
}
type Execution struct {
	JobProject         string    `json:"jobProject"`
	Principal          Principal `json:"principal"`
	MaximumBytesBilled string    `json:"maximumBytesBilled"`
	SessionBudgetBytes string    `json:"sessionBudgetBytes"`
}
type Bounds struct {
	PageSize           int   `json:"pageSize"`
	MaxRows            int   `json:"maxRows"`
	MaxPages           int   `json:"maxPages"`
	ResponseBytes      int   `json:"responseBytes"`
	TotalResponseBytes int64 `json:"totalResponseBytes"`
	WallMs             int   `json:"wallMs"`
	HTTPMs             int   `json:"httpMs"`
	Concurrency        int   `json:"concurrency"`
}

func DefaultBounds() Bounds { return Bounds{100, 1000, 100, 4 << 20, 32 << 20, 120000, 15000, 1} }
func (b Bounds) validate() error {
	if b.PageSize < 1 || b.PageSize > 1000 || b.MaxRows < 1 || b.MaxRows > 10000 || b.MaxPages < 1 || b.MaxPages > 100 || b.ResponseBytes < 1 || b.ResponseBytes > MaxResponseBytes || b.TotalResponseBytes < 1 || b.TotalResponseBytes > 32<<20 || b.WallMs < 1 || b.WallMs > 120000 || b.HTTPMs < 1 || b.HTTPMs > 15000 || b.Concurrency != 1 {
		return fail("invalid_input")
	}
	return nil
}

type TableRef struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	TableID   string `json:"tableId"`
}
type JobRef struct {
	ProjectID string `json:"projectId"`
	JobID     string `json:"jobId"`
	Location  string `json:"location"`
}
type Observation struct {
	Table        TableRef       `json:"table"`
	Location     string         `json:"location"`
	Type         string         `json:"type"`
	Config       map[string]any `json:"config"`
	Schema       []Field        `json:"schema"`
	Digest       string         `json:"digest"`
	ObservedAt   time.Time      `json:"observedAt"`
	Etag         string         `json:"etag,omitempty"`
	LastModified string         `json:"lastModified,omitempty"`
}
type Preview struct {
	Nonce          string      `json:"nonce"`
	Plan           ReadPlan    `json:"plan"`
	Observation    Observation `json:"observation"`
	PolicyDigest   string      `json:"policyDigest"`
	Execution      Execution   `json:"execution"`
	Bounds         Bounds      `json:"bounds"`
	EstimatedBytes string      `json:"estimatedBytes"`
	CreatedAt      time.Time   `json:"createdAt"`
	ExpiresAt      time.Time   `json:"expiresAt"`
	ApprovalDigest string      `json:"approvalDigest"`
}

// Approval cannot be manufactured from JSON or a caller boolean. Execute reloads
// its nonce and immutable binding from the original private ledger.
type Approval struct {
	nonce, digest string
	client        *Client
}
type Counters struct {
	Rows  int   `json:"rows"`
	Pages int   `json:"pages"`
	Bytes int64 `json:"bytes"`
}
type Receipt struct {
	Version                       int       `json:"version"`
	RunID                         string    `json:"runId"`
	ApprovalDigest                string    `json:"approvalDigest"`
	SourceDigest                  string    `json:"sourceDigest"`
	ObservationDigest             string    `json:"observationDigest"`
	SchemaDigest                  string    `json:"schemaDigest"`
	Principal                     Principal `json:"principal"`
	Job                           *JobRef   `json:"job,omitempty"`
	State                         string    `json:"state"`
	RunStartedAt                  time.Time `json:"runStartedAt"`
	ExecutionDeadline             time.Time `json:"executionDeadline"`
	Bounds                        Bounds    `json:"bounds"`
	Counters                      Counters  `json:"counters"`
	ProcessedBytes                *string   `json:"processedBytes,omitempty"`
	BilledBytes                   *string   `json:"billedBytes,omitempty"`
	CacheHit                      *bool     `json:"cacheHit,omitempty"`
	Warnings                      []string  `json:"warnings"`
	Reason                        string    `json:"reason,omitempty"`
	LocalStopped                  bool      `json:"localStopped"`
	ResidualSourceReplacementRace bool      `json:"residualSourceReplacementRace"`
}
type Page struct {
	Schema  []Field  `json:"schema"`
	Rows    [][]Cell `json:"rows"`
	Receipt Receipt  `json:"receipt"`
	Cursor  string   `json:"cursor"`
}
type JobStatus struct {
	Job         JobRef   `json:"job"`
	State       string   `json:"state"`
	BilledBytes *string  `json:"billedBytes,omitempty"`
	Warnings    []string `json:"warnings"`
}
type CancelResult struct {
	Job   JobRef `json:"job"`
	State string `json:"state"`
}

// Identity is attested by the operator's provider, never by request JSON. Read
// and Cancel describe already granted capabilities; the SDK requests no scopes.
type Identity struct {
	Principal    Principal
	ExpiresAt    time.Time
	Read, Cancel bool
}

// Provider composes authentication ABOVE the supplied guarded dispatch transport.
// Retry-capable authentication middleware cannot bypass its POST attempt gate.
type Provider interface {
	Authorize(context.Context, http.RoundTripper) (Identity, http.RoundTripper, error)
}
type Clock interface {
	Now() time.Time
	Sleep(context.Context, time.Duration) error
}
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) Sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return fail("local_stopped")
	}
}

// Prepare reauthorizes the ORIGINAL protected query/context on every operation.
// The consumer supplies this trusted policy bridge; a cursor grants no access.
type Prepare func(context.Context) (ReadPlan, string, error)
type Config struct {
	Profiles  []SourceProfile
	Provider  Provider
	Transport http.RoundTripper
	Ledger    Ledger
	Prepare   Prepare
	Clock     Clock
}

func jsonCopy[T any](v T) (T, error) {
	var out T
	b, e := json.Marshal(v)
	if e == nil {
		e = json.Unmarshal(b, &out)
	}
	if e != nil {
		return out, fail("invalid_input")
	}
	return out, nil
}
func digestObject(name string, v any) (string, error) {
	raw, e := json.Marshal(v)
	if e != nil {
		return "", fail("invalid_input")
	}
	d, _, e := HashPayload(name, raw)
	return d, e
}
