package bigquery

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"time"
)

// SnapshotResult contains current public receipt state and the existing opaque
// delivery cursor, if one was issued. It contains no rows or private plan values.
type SnapshotResult struct {
	Receipt Receipt `json:"receipt"`
	Cursor  string  `json:"cursor"`
}

// Snapshot reads authoritative local recovery state without provider/policy
// preparation, HTTP, row delivery or ledger mutation. Immutable receipt authority
// must match the trusted run. Mutable state/counters come only from the ledger.
// The existing cursor is returned unchanged, never minted or renewed. Snapshot
// remains available after execution expiry; it grants no execution authority.
// Use the same caller control context for a control operation and its snapshot.
// The entire local operation has a ceiling of 15 seconds; a busy run lease fails
// immediately. Operator-owned filesystem primitives must still return.
func (c *Client) Snapshot(ctx context.Context, receipt Receipt) (SnapshotResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	read := func(out *SnapshotResult) error {
		return c.ledger.readContext(ctx, func(state *ledgerState) error {
			record, ok := state.Runs[receipt.RunID]
			if !ok || !receiptMatches(receipt, record.Receipt) {
				return fail("cursor_invalid")
			}
			if out == nil {
				return nil
			}
			copied, e := jsonCopy(record.Receipt)
			if e != nil {
				return e
			}
			out.Receipt = copied
			if record.Cursor != nil {
				raw, e := json.Marshal(record.Cursor)
				if e != nil {
					return e
				}
				out.Cursor = base64.RawURLEncoding.EncodeToString(raw)
			}
			return nil
		})
	}
	// Reject forged/unknown authority before asking a FileLedger to open a lease.
	if e := read(nil); e != nil {
		return SnapshotResult{}, e
	}
	release, e := c.ledger.lease(ctx, receipt.RunID)
	if e != nil {
		return SnapshotResult{}, e
	}
	defer release()
	var result SnapshotResult
	if e = read(&result); e != nil {
		return SnapshotResult{}, e
	}
	if ctx.Err() != nil {
		return SnapshotResult{}, deadlineError()
	}
	return result, nil
}
