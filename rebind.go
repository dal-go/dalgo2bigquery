package bigquery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"
)

type RebindResult struct {
	Receipt Receipt `json:"receipt"`
	Cursor  string  `json:"cursor"`
}

// preparation includes trusted identity/policy work in the same operation bound.
// An expired run may prepare only recovery of its existing job, never fetch rows.
func (c *Client) preparation(ctx context.Context, scope *operationScope, control bool) (context.Context, context.CancelFunc, error) {
	remaining, e := c.remaining(scope)
	if e != nil {
		return nil, nil, e
	}
	caller := c.callerDeadline(ctx)
	deadline, e := OperationDeadline(c.clock.Now(), scope.deadline, caller, time.Duration(scope.bounds.HTTPMs)*time.Millisecond, control, remaining)
	if e != nil {
		return nil, nil, e
	}
	if control {
		scope.controlDeadline = deadline
	}
	bounded, cancel := context.WithTimeout(ctx, deadline.Sub(c.clock.Now()))
	bounded = context.WithValue(bounded, clockBoundKey{}, deadline)
	return bounded, cancel, nil
}

// RebindJob verifies a fresh trusted generation for the SAME original subject.
// It does not renew approval, submit/fetch a job, or change receipt provenance,
// offsets, reservations, counters or the original execution deadline. After
// that deadline only existing bounded Status/CancelJob recovery remains usable.
func (c *Client) RebindJob(ctx context.Context, receipt Receipt, encoded string) (RebindResult, error) {
	rr, e := c.loadRun(receipt.RunID)
	if e != nil {
		return RebindResult{}, e
	}
	release, e := c.ledger.lease(ctx, rr.Receipt.RunID)
	if e != nil {
		return RebindResult{}, e
	}
	defer release()
	rr, e = c.loadRun(rr.Receipt.RunID)
	if e != nil {
		return RebindResult{}, e
	}
	if rr.Receipt.Job == nil || !receiptMatches(receipt, rr.Receipt) {
		return RebindResult{}, fail("cursor_invalid")
	}
	// Cursor authority is ledger-backed; even a correctly signed older generation
	// or offset cannot win a concurrent rebind/delivery race.
	if rr.Cursor != nil {
		raw, _ := json.Marshal(rr.Cursor)
		if encoded != base64.RawURLEncoding.EncodeToString(raw) {
			return RebindResult{}, fail("cursor_invalid")
		}
	} else if encoded != "" {
		return RebindResult{}, fail("cursor_invalid")
	}
	scope := runScope(rr)
	expired := !c.clock.Now().Before(rr.Receipt.ExecutionDeadline)
	started := c.clock.Now()

	bounded, cancel, e := c.preparation(ctx, scope, expired)
	if e != nil {
		return RebindResult{}, e
	}
	defer cancel()
	if e = c.reauthorize(bounded, rr.Preview.Plan, rr.Preview.PolicyDigest); e != nil {
		return RebindResult{}, e
	}
	identity, _, e := c.authorize(bounded, rejectTransport{})
	if e != nil {
		return RebindResult{}, sanitizedAuth(e)
	}
	if !validPrincipal(identity.Principal) {
		return RebindResult{}, fail("auth_required")
	}
	if !identity.Read {
		return RebindResult{}, fail("scope_missing")
	}
	if !c.clock.Now().Before(identity.ExpiresAt) {
		return RebindResult{}, fail("auth_expired")
	}
	original := rr.Receipt.Principal
	if identity.Principal.Kind != original.Kind || identity.Principal.Subject != original.Subject || identity.Principal.Generation == rr.ActivePrincipal.Generation {
		return RebindResult{}, fail("approval_changed")
	}
	deadline := started.Add(time.Duration(scope.bounds.HTTPMs) * time.Millisecond)
	if expired {
		deadline = scope.controlDeadline
	} else if rr.Receipt.ExecutionDeadline.Before(deadline) {
		deadline = rr.Receipt.ExecutionDeadline
	}
	if bounded.Err() != nil || !c.clock.Now().Before(deadline) {
		return RebindResult{}, deadlineError()
	}
	var result RebindResult
	e = c.mutateRunContext(bounded, rr.Receipt.RunID, func(current *runRecord) error {
		if current.ActivePrincipal != rr.ActivePrincipal || !equalJSON(current.Cursor, rr.Cursor) {
			return fail("cursor_invalid")
		}
		current.ActivePrincipal = identity.Principal
		if current.Cursor != nil {
			current.Cursor.Principal = identity.Principal
			current.Cursor.LedgerRef = opaqueID()
		} else {
			current.Cursor = &cursorState{1, rr.Receipt.RunID, *rr.Receipt.Job, rr.Preview.Plan.Digest, rr.Receipt.SchemaDigest, identity.Principal, rr.PageToken, rr.Receipt.Bounds.PageSize, rr.Offset, rr.PageDigest, rr.PageOrdinal, rr.Receipt.Counters, opaqueID()}
		}
		raw, _ := json.Marshal(current.Cursor)
		result = RebindResult{current.Receipt, base64.RawURLEncoding.EncodeToString(raw)}
		return nil
	})
	return result, e
}

// Context deadlines use the wall clock; translate remaining duration into the
// injected clock domain so deterministic restart/expiry tests retain the same bound.
func (c *Client) callerDeadline(ctx context.Context) time.Time {
	if d, ok := ctx.Deadline(); ok {
		return c.clock.Now().Add(time.Until(d))
	}
	return time.Time{}
}

type clockBoundKey struct{}

func (c *Client) checkBound(ctx context.Context) error {
	if ctx.Err() != nil {
		return fail("local_stopped")
	}
	if d, ok := ctx.Value(clockBoundKey{}).(time.Time); ok && !c.clock.Now().Before(d) {
		return deadlineError()
	}
	return nil
}
