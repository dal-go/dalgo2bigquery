package bigquery

import (
	"context"
	"encoding/json"
	bq "google.golang.org/api/bigquery/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Client struct {
	profiles  map[string]SourceProfile
	provider  Provider
	transport http.RoundTripper
	ledger    Ledger
	prepare   Prepare
	clock     Clock
}

func NewClient(cfg Config) (*Client, error) {
	if cfg.Provider == nil || cfg.Transport == nil || cfg.Ledger == nil || cfg.Prepare == nil || len(cfg.Profiles) < 1 {
		return nil, fail("invalid_input")
	}
	clock := cfg.Clock
	if clock == nil {
		clock = realClock{}
	}
	c := &Client{map[string]SourceProfile{}, cfg.Provider, cfg.Transport, cfg.Ledger, cfg.Prepare, clock}
	for _, p := range cfg.Profiles {
		p, e := jsonCopy(p)
		if e != nil {
			return nil, e
		}
		d, e := sourceDigest(p)
		if e != nil {
			return nil, e
		}
		if _, exists := c.profiles[d]; exists {
			return nil, fail("invalid_input")
		}
		c.profiles[d] = p
	}
	return c, nil
}
func (c *Client) validatePlan(plan ReadPlan) (SourceProfile, ReadPlan, error) {
	// JSON freezing rejects cycles and prevents caller mutation after preparation.
	plan, e := jsonCopy(plan)
	if e != nil {
		return SourceProfile{}, ReadPlan{}, e
	}
	p, ok := c.profiles[plan.SourceDigest]
	if !ok || plan.Version != 1 || plan.Limit < 1 || plan.Limit > 10000 || len(plan.Projection) < 1 || len(plan.Projection) > 128 || len(plan.Order) > 16 || len(plan.Parameters) > 128 {
		return p, plan, fail("invalid_input")
	}
	raw, _ := json.Marshal(plan)
	if len(raw) > 256<<10 {
		return p, plan, fail("invalid_input")
	}
	fs := map[string]Field{}
	for _, f := range p.Schema {
		fs[f.Name] = f
	}
	seen := map[string]bool{}
	for _, n := range plan.Projection {
		f, ok := fs[n]
		if !ok || seen[n] || !scalarType(canonicalType(f.Type)) || f.Mode == "REPEATED" || len(f.Fields) > 0 {
			return p, plan, fail("unsupported_query")
		}
		seen[n] = true
	}
	for _, o := range plan.Order {
		f, ok := fs[o.Column]
		if !ok || !ordered(f) || (o.Direction != "ASC" && o.Direction != "DESC") {
			return p, plan, fail("unsupported_query")
		}
	}
	params := map[string]Parameter{}
	for i, param := range plan.Parameters {
		if param.Name != "p"+strconv.Itoa(i) {
			return p, plan, fail("invalid_input")
		}
		params[param.Name] = param
	}
	used := map[string]bool{}
	count := 0
	var visit func(*Predicate, int) error
	visit = func(pred *Predicate, depth int) error {
		if pred == nil {
			return nil
		}
		count++
		if count > 128 || depth >= 8 {
			return fail("unsupported_query")
		}
		if pred.Op == "AND" || pred.Op == "OR" {
			if pred.Column != "" || pred.Parameter != "" || len(pred.Children) < 1 {
				return fail("unsupported_query")
			}
			for i := range pred.Children {
				if e := visit(&pred.Children[i], depth+1); e != nil {
					return e
				}
			}
			return nil
		}
		f, ok := fs[pred.Column]
		if !ok || !scalarType(canonicalType(f.Type)) || f.Mode == "REPEATED" || len(f.Fields) > 0 || len(pred.Children) > 0 {
			return fail("unsupported_query")
		}
		if pred.Op == "IS NULL" || pred.Op == "IS NOT NULL" {
			if pred.Parameter != "" {
				return fail("invalid_input")
			}
			return nil
		}
		if f.Type == "JSON" || f.Type == "BYTES" {
			return fail("unsupported_query")
		}
		switch pred.Op {
		case "=", "!=", "IN":
		case "<", "<=", ">", ">=":
			if !ordered(f) {
				return fail("unsupported_query")
			}
		default:
			return fail("unsupported_query")
		}
		param, ok := params[pred.Parameter]
		if !ok || used[pred.Parameter] {
			return fail("invalid_input")
		}
		used[pred.Parameter] = true
		if pred.Op == "IN" {
			if param.Type != "ARRAY<"+canonicalType(f.Type)+">" {
				return fail("invalid_input")
			}
			a, ok := param.Value.([]any)
			if !ok || len(a) > 1000 {
				return fail("invalid_input")
			}
			for _, v := range a {
				if v == nil {
					return fail("invalid_input")
				}
				n, e := parameterValue(f, v)
				if e != nil || !equalJSON(n, v) {
					return fail("invalid_input")
				}
			}
		} else {
			if param.Type != canonicalType(f.Type) || param.Value == nil {
				return fail("invalid_input")
			}
			v, e := parameterValue(f, param.Value)
			if e != nil || !equalJSON(v, param.Value) {
				return fail("invalid_input")
			}
		}
		return nil
	}
	if e = visit(plan.Where, 0); e != nil {
		return p, plan, e
	}
	if len(used) != len(params) {
		return p, plan, fail("invalid_input")
	}
	sql, e := renderPlan(p, plan)
	if e != nil || sql != plan.SQL {
		return p, plan, fail("invalid_input")
	}
	d, e := digestObject("ReadPlan", plan)
	if e != nil || d != plan.Digest {
		return p, plan, fail("approval_changed")
	}
	return p, plan, nil
}
func (c *Client) reauthorize(ctx context.Context, plan ReadPlan, policy string) error {
	if e := c.checkBound(ctx); e != nil {
		return e
	}
	current, p, e := c.prepare(ctx)
	if bound := c.checkBound(ctx); bound != nil {
		return bound
	}
	if e != nil {
		return sanitizedAuth(e)
	}
	_, current, e = c.validatePlan(current)
	if e != nil {
		return e
	}
	if current.Digest != plan.Digest || p != policy {
		return fail("approval_changed")
	}
	return nil
}
func approvalDigest(p Preview) (string, error) {
	return digestObject("Approval", map[string]any{"effectivePlanDigest": p.Plan.Digest, "observationDigest": p.Observation.Digest, "policyDigest": p.PolicyDigest, "principal": p.Execution.Principal, "jobProject": p.Execution.JobProject, "location": p.Observation.Location, "maximumBytesBilled": p.Execution.MaximumBytesBilled, "sessionBudgetBytes": p.Execution.SessionBudgetBytes, "bounds": p.Bounds, "estimatedBytes": p.EstimatedBytes})
}
func (c *Client) Preview(ctx context.Context, plan ReadPlan, execution Execution, bounds Bounds) (Preview, error) {
	profile, plan, e := c.validatePlan(plan)
	if e != nil {
		return Preview{}, e
	}
	if e = bounds.validate(); e != nil {
		return Preview{}, e
	}
	cap, e := positiveBytes(execution.MaximumBytesBilled)
	if e != nil {
		return Preview{}, e
	}
	budget, e := positiveBytes(execution.SessionBudgetBytes)
	if e != nil || budget < cap || !projectID.MatchString(execution.JobProject) || !validPrincipal(execution.Principal) {
		return Preview{}, fail("invalid_input")
	}
	current, policy, e := c.prepare(ctx)
	if e != nil {
		return Preview{}, sanitizedAuth(e)
	}
	_, current, e = c.validatePlan(current)
	if e != nil {
		return Preview{}, e
	}
	if current.Digest != plan.Digest {
		return Preview{}, fail("approval_changed")
	}
	scope := newPreviewScope(bounds, c.clock.Now())
	obs, e := c.observe(ctx, profile, scope, &execution.Principal)
	if e != nil {
		return Preview{}, e
	}
	estimate, e := c.estimate(ctx, plan, execution, obs.Location, scope)
	if e != nil {
		return Preview{}, e
	}
	created := c.clock.Now()
	preview := Preview{opaqueID(), plan, obs, policy, execution, bounds, estimate, created, created.Add(5 * time.Minute), ""}
	preview.ApprovalDigest, e = approvalDigest(preview)
	if e != nil {
		return Preview{}, e
	}
	e = c.ledger.update(func(s *ledgerState) error { s.Previews[preview.Nonce] = previewRecord{Preview: preview}; return nil })
	return preview, e
}
func (c *Client) Approve(preview Preview, digest string) (Approval, error) {
	if digest == "" || digest != preview.ApprovalDigest {
		return Approval{}, fail("approval_required")
	}
	e := c.ledger.update(func(s *ledgerState) error {
		stored, ok := s.Previews[preview.Nonce]
		if !ok || stored.Used {
			return fail("approval_required")
		}
		if !equalJSON(stored.Preview, preview) || !c.clock.Now().Before(stored.Preview.ExpiresAt) {
			return fail("approval_changed")
		}
		return nil
	})
	if e != nil {
		return Approval{}, e
	}
	return Approval{preview.Nonce, digest, c}, nil
}
func queryRequest(plan ReadPlan, execution Execution, bounds Bounds, location string, dry bool) (*bq.QueryRequest, error) {
	cap, e := positiveBytes(execution.MaximumBytesBilled)
	if e != nil {
		return nil, e
	}
	legacy := false
	request := &bq.QueryRequest{Query: plan.SQL, Location: location, UseLegacySql: &legacy, DryRun: dry, MaximumBytesBilled: cap, ParameterMode: "NAMED", MaxResults: int64(bounds.PageSize), TimeoutMs: 1000, JobCreationMode: "JOB_CREATION_REQUIRED", JobTimeoutMs: int64(bounds.WallMs), FormatOptions: &bq.DataFormatOptions{UseInt64Timestamp: true}, ForceSendFields: []string{"DryRun", "QueryParameters"}}
	for _, p := range plan.Parameters {
		typ := &bq.QueryParameterType{Type: p.Type}
		value := &bq.QueryParameterValue{}
		if strings.HasPrefix(p.Type, "ARRAY<") {
			typ.Type = "ARRAY"
			typ.ArrayType = &bq.QueryParameterType{Type: strings.TrimSuffix(strings.TrimPrefix(p.Type, "ARRAY<"), ">")}
			a, ok := p.Value.([]any)
			if !ok {
				return nil, fail("invalid_input")
			}
			value.ForceSendFields = []string{"ArrayValues"}
			for _, v := range a {
				scalar, e := sdkParameterValue(v)
				if e != nil {
					return nil, e
				}
				value.ArrayValues = append(value.ArrayValues, scalar)
			}
		} else {
			value, e = sdkParameterValue(p.Value)
			if e != nil {
				return nil, e
			}
		}
		request.QueryParameters = append(request.QueryParameters, &bq.QueryParameter{Name: p.Name, ParameterType: typ, ParameterValue: value})
	}
	return request, nil
}
func sdkParameterValue(v any) (*bq.QueryParameterValue, error) {
	if v == nil {
		return &bq.QueryParameterValue{NullFields: []string{"Value"}}, nil
	}
	s, ok := v.(string)
	if !ok {
		if b, yes := v.(bool); yes {
			s = strconv.FormatBool(b)
		} else {
			return nil, fail("invalid_input")
		}
	}
	return &bq.QueryParameterValue{Value: s, ForceSendFields: []string{"Value"}}, nil
}
func (c *Client) estimate(ctx context.Context, plan ReadPlan, execution Execution, location string, scope *operationScope) (string, error) {
	req, e := queryRequest(plan, execution, scope.bounds, location, true)
	if e != nil {
		return "", e
	}
	raw, e := c.call(ctx, scope, &execution.Principal, false, func(api sdkAPI) error {
		_, e := api.service.Jobs.Query(execution.JobProject, req).Context(api.ctx).Do()
		return e
	})
	if e != nil {
		return "", e
	}
	m, e := object(raw)
	if e != nil {
		return "", e
	}
	if errs, ok := m["errors"]; ok {
		a, ok := errs.([]any)
		if !ok || len(a) > 0 {
			return "", fail("remote_failed")
		}
	}
	est, e := decimalString(m, "totalBytesProcessed")
	if e != nil || est == nil {
		return "", fail("estimate_missing")
	}
	cap, _ := positiveBytes(execution.MaximumBytesBilled)
	n, _ := strconv.ParseInt(*est, 10, 64)
	if n > cap {
		return "", fail("cap_exceeded")
	}
	return *est, nil
}
