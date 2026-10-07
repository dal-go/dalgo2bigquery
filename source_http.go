package bigquery

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// sourceTransport confines an authorized credential to this dataset's metadata
// and physical row GET routes. It is below the provider's auth transport, so
// an auth middleware cannot turn a source read into a query or mutation.
type sourceTransport struct {
	base             http.RoundTripper
	project, dataset string
}

func (t sourceTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	base := "/bigquery/v2/projects/" + t.project + "/datasets/" + t.dataset
	if req.Method != http.MethodGet || req.Body != nil || req.URL.Scheme != "https" || req.URL.Host != "bigquery.googleapis.com" || req.URL.User != nil || req.URL.Fragment != "" || req.URL.EscapedPath() != req.URL.Path {
		return nil, fail("invalid_input")
	}
	path := req.URL.Path
	q := req.URL.Query()
	allowed := map[string]bool{"alt": true, "prettyPrint": true}
	switch {
	case path == base:
		allowed["datasetView"] = true
		if q.Get("datasetView") != "METADATA" {
			return nil, fail("invalid_input")
		}
	case path == base+"/tables":
		allowed["maxResults"], allowed["pageToken"] = true, true
	case strings.HasPrefix(path, base+"/tables/"):
		rest := strings.TrimPrefix(path, base+"/tables/")
		name, isData := strings.CutSuffix(rest, "/data")
		if !isData {
			name = rest
		}
		if !identifier.MatchString(name) || strings.Contains(name, "/") {
			return nil, fail("invalid_input")
		}
		if isData {
			allowed["maxResults"], allowed["pageToken"], allowed["formatOptions.useInt64Timestamp"] = true, true, true
			if q.Get("formatOptions.useInt64Timestamp") != "true" {
				return nil, fail("invalid_input")
			}
		} else {
			allowed["view"] = true
			if q.Get("view") != "STORAGE_STATS" {
				return nil, fail("invalid_input")
			}
		}
	default:
		return nil, fail("invalid_input")
	}
	for k, v := range q {
		if !allowed[k] || len(v) != 1 || (k == "alt" && v[0] != "json") || (k == "prettyPrint" && v[0] != "false") {
			return nil, fail("invalid_input")
		}
		if k == "maxResults" {
			n, e := strconv.Atoi(v[0])
			if e != nil || n < 1 || n > 1000 {
				return nil, fail("invalid_input")
			}
		}
		if k == "pageToken" && (v[0] == "" || len(v[0]) > 4096) {
			return nil, fail("invalid_input")
		}
	}
	return t.base.RoundTrip(req)
}

func (d *sourceDatabase) client() *Client {
	return &Client{provider: d.provider, transport: sourceTransport{base: d.transport, project: d.project, dataset: d.dataset}, clock: d.clock}
}

func (d *sourceDatabase) identity(ctx context.Context) (Principal, error) {
	c := d.client()
	i, _, e := c.authorize(ctx, rejectTransport{})
	if e != nil {
		return Principal{}, sanitizedAuth(e)
	}
	if !validPrincipal(i.Principal) || !i.Read {
		return Principal{}, fail("auth_required")
	}
	if !d.clock.Now().Before(i.ExpiresAt) {
		return Principal{}, fail("auth_expired")
	}
	return i.Principal, nil
}

func (d *sourceDatabase) get(ctx context.Context, principal Principal, call func(sdkAPI) error) (map[string]any, error) {
	m, _, e := d.getWithBytes(ctx, principal, call)
	return m, e
}

func (d *sourceDatabase) getWithBytes(ctx context.Context, principal Principal, call func(sdkAPI) error) (map[string]any, int64, error) {
	bounds := DefaultBounds()
	bounds.ResponseBytes = d.limits.ResponseBytes
	bounds.TotalResponseBytes = int64(d.limits.ResponseBytes)
	scope := newPreviewScope(bounds, d.clock.Now())
	raw, e := d.client().call(ctx, scope, &principal, false, call)
	if e != nil {
		return nil, scope.bytes, e
	}
	m, e := object(raw)
	return m, scope.bytes, e
}

func (d *sourceDatabase) datasetMetadata(ctx context.Context, principal Principal) error {
	m, e := d.get(ctx, principal, func(api sdkAPI) error {
		_, e := api.service.Datasets.Get(d.project, d.dataset).DatasetView("METADATA").Context(api.ctx).Do()
		return e
	})
	if e != nil {
		return e
	}
	ref, e := object(m["datasetReference"])
	if e != nil || ref["projectId"] != d.project || ref["datasetId"] != d.dataset {
		return fail("source_changed")
	}
	return nil
}

func (d *sourceDatabase) tableMetadata(ctx context.Context, principal Principal, name string) (map[string]any, error) {
	if !identifier.MatchString(name) {
		return nil, fail("invalid_input")
	}
	m, e := d.get(ctx, principal, func(api sdkAPI) error {
		_, e := api.service.Tables.Get(d.project, d.dataset, name).View("STORAGE_STATS").Context(api.ctx).Do()
		return e
	})
	if e != nil {
		return nil, e
	}
	ref, e := object(m["tableReference"])
	if e != nil || ref["projectId"] != d.project || ref["datasetId"] != d.dataset || ref["tableId"] != name {
		return nil, fail("source_changed")
	}
	return m, nil
}

func sourceToken(m map[string]any, key string) (string, error) {
	v, ok := m[key]
	if !ok {
		return "", nil
	}
	s, ok := v.(string)
	if !ok || len(s) > 4096 {
		return "", fail("malformed_wire")
	}
	return s, nil
}

func sourceCount(m map[string]any, key string) (int64, error) {
	s, ok := m[key].(string)
	if !ok {
		return 0, fail("malformed_wire")
	}
	n, e := strconv.ParseInt(s, 10, 64)
	if e != nil || n < 0 {
		return 0, fail("malformed_wire")
	}
	return n, nil
}

func sourceDeadline(ctx context.Context, limit time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, limit)
}
