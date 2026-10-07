package bigquery

import (
	"context"
	"encoding/base64"
	"io"
	"strconv"
	"time"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/dbschema"
)

type sourceRowCursor struct {
	db               *sourceDatabase
	ctx              context.Context
	cancel           context.CancelFunc
	principal        Principal
	name             string
	fields           []Field
	etag, modified   string
	expected         int64
	rows             []dbschema.SourceRow
	index            int
	token            string
	seen             map[string]bool
	page             int
	count            int64
	bytes            int64
	finished, closed bool
	terminal         error
}

func (d *ReadOnlyDatabase) OpenSourceRows(ctx context.Context, ref *dal.CollectionRef) (dbschema.SourceRowCursor, error) {
	if ref == nil || ref.Schema() != "" || !identifier.MatchString(ref.Name()) {
		return nil, fail("invalid_input")
	}
	ctx, cancel := sourceDeadline(ctx, d.source.limits.WallTime)
	principal, e := d.source.identity(ctx)
	if e != nil {
		cancel()
		return nil, e
	}
	if e = d.source.datasetMetadata(ctx, principal); e != nil {
		cancel()
		return nil, e
	}
	m, e := d.source.tableMetadata(ctx, principal, ref.Name())
	if e != nil {
		cancel()
		return nil, e
	}
	if m["type"] != "TABLE" || m["streamingBuffer"] != nil {
		cancel()
		return nil, fail("source_ineligible")
	}
	fields, e := sourceFields(m)
	if e != nil {
		cancel()
		return nil, e
	}
	count, e := sourceCount(m, "numRows")
	if e != nil {
		cancel()
		return nil, e
	}
	if count > d.source.limits.MaxRows {
		cancel()
		return nil, fail("response_limit")
	}
	etag, e := requiredString(m, "etag")
	if e != nil {
		cancel()
		return nil, e
	}
	modified, e := requiredString(m, "lastModifiedTime")
	if e != nil {
		cancel()
		return nil, e
	}
	return &sourceRowCursor{db: d.source, ctx: ctx, cancel: cancel, principal: principal, name: ref.Name(), fields: fields, etag: etag, modified: modified, expected: count, seen: map[string]bool{}}, nil
}

func (c *sourceRowCursor) Next() (dbschema.SourceRow, error) {
	if c.terminal != nil {
		return dbschema.SourceRow{}, c.terminal
	}
	if c.closed || c.finished {
		return dbschema.SourceRow{}, io.EOF
	}
	if c.ctx.Err() != nil {
		return dbschema.SourceRow{}, c.fail(deadlineError())
	}
	for c.index == len(c.rows) {
		if c.page > 0 && c.token == "" {
			if c.count != c.expected {
				return dbschema.SourceRow{}, c.fail(fail("source_changed"))
			}
			if e := c.verifyFinal(); e != nil {
				return dbschema.SourceRow{}, c.fail(e)
			}
			c.finished = true
			c.cancel()
			return dbschema.SourceRow{}, io.EOF
		}
		if e := c.loadPage(); e != nil {
			return dbschema.SourceRow{}, c.fail(e)
		}
	}
	row := c.rows[c.index]
	c.index++
	c.count++
	if c.count > c.expected || c.count > c.db.limits.MaxRows {
		return dbschema.SourceRow{}, c.fail(fail("source_changed"))
	}
	return row, nil
}

func (c *sourceRowCursor) fail(err error) error {
	c.terminal = err
	c.rows = nil
	c.cancel()
	return err
}

func (c *sourceRowCursor) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.rows = nil
	c.cancel()
	return nil
}

func (c *sourceRowCursor) loadPage() error {
	if c.page >= c.db.limits.MaxPages {
		return fail("response_limit")
	}
	previous := c.token
	m, bytes, e := c.db.getWithBytes(c.ctx, c.principal, func(api sdkAPI) error {
		call := api.service.Tabledata.List(c.db.project, c.db.dataset, c.name).MaxResults(int64(c.db.limits.PageSize)).FormatOptionsUseInt64Timestamp(true)
		if previous != "" {
			call = call.PageToken(previous)
		}
		_, e := call.Context(api.ctx).Do()
		return e
	})
	if e != nil {
		return e
	}
	c.bytes += bytes
	if c.bytes > c.db.limits.MaxBytes {
		return fail("response_limit")
	}
	if m["kind"] != "bigquery#tableDataList" {
		return fail("malformed_wire")
	}
	total, e := sourceCount(m, "totalRows")
	if e != nil {
		return e
	}
	if total != c.expected {
		return fail("source_changed")
	}
	items, ok := m["rows"].([]any)
	if !ok && m["rows"] != nil {
		return fail("malformed_wire")
	}
	rows := make([]dbschema.SourceRow, 0, len(items))
	for _, raw := range items {
		row, e := sourceDecodeRow(raw, c.fields)
		if e != nil {
			return e
		}
		rows = append(rows, row)
	}
	if c.count+int64(len(rows)) > c.expected {
		return fail("source_changed")
	}
	next, e := sourceToken(m, "pageToken")
	if e != nil {
		return e
	}
	if next != "" {
		if next == previous || c.seen[next] {
			return fail("cursor_invalid")
		}
		c.seen[next] = true
	}
	c.rows, c.index, c.token = rows, 0, next
	c.page++
	return nil
}

func (c *sourceRowCursor) verifyFinal() error {
	m, e := c.db.tableMetadata(c.ctx, c.principal, c.name)
	if e != nil {
		return e
	}
	if m["type"] != "TABLE" || m["streamingBuffer"] != nil || m["etag"] != c.etag || m["lastModifiedTime"] != c.modified {
		return fail("source_changed")
	}
	count, e := sourceCount(m, "numRows")
	if e != nil || count != c.expected {
		return fail("source_changed")
	}
	fields, e := sourceFields(m)
	if e != nil || !equalJSON(fields, c.fields) {
		return fail("source_changed")
	}
	return c.db.client().checkIdentity(c.ctx, c.principal)
}

func sourceDecodeRow(raw any, fields []Field) (dbschema.SourceRow, error) {
	m, e := object(raw)
	if e != nil {
		return dbschema.SourceRow{}, e
	}
	a, ok := m["f"].([]any)
	if !ok || len(a) != len(fields) {
		return dbschema.SourceRow{}, fail("malformed_wire")
	}
	values := make(map[string]any, len(fields))
	for i, item := range a {
		cell, e := object(item)
		if e != nil {
			return dbschema.SourceRow{}, e
		}
		v, ok := cell["v"]
		if !ok {
			return dbschema.SourceRow{}, fail("malformed_wire")
		}
		normalized, e := NormalizeScalar(fields[i], v)
		if e != nil {
			return dbschema.SourceRow{}, e
		}
		var result any = normalized.Value
		if normalized.Value != nil {
			s, ok := normalized.Value.(string)
			switch fields[i].Type {
			case "INT64":
				if !ok {
					return dbschema.SourceRow{}, fail("malformed_wire")
				}
				result, e = strconv.ParseInt(s, 10, 64)
			case "NUMERIC", "BIGNUMERIC", "STRING", "DATE", "TIME", "DATETIME", "JSON":
				if !ok {
					return dbschema.SourceRow{}, fail("malformed_wire")
				}
			case "FLOAT64":
				if !ok {
					return dbschema.SourceRow{}, fail("malformed_wire")
				}
				result, e = strconv.ParseFloat(s, 64)
			case "BYTES":
				if !ok {
					return dbschema.SourceRow{}, fail("malformed_wire")
				}
				result, e = base64.StdEncoding.Strict().DecodeString(s)
			case "TIMESTAMP":
				if !ok {
					return dbschema.SourceRow{}, fail("malformed_wire")
				}
				micros, parseErr := strconv.ParseInt(s, 10, 64)
				if parseErr != nil {
					e = parseErr
				} else {
					result = time.UnixMicro(micros).UTC()
				}
			case "BOOL":
				_, ok = normalized.Value.(bool)
				if !ok {
					return dbschema.SourceRow{}, fail("malformed_wire")
				}
			default:
				return dbschema.SourceRow{}, fail("unsupported_type")
			}
			if e != nil {
				return dbschema.SourceRow{}, fail("malformed_wire")
			}
		}
		values[fields[i].Name] = result
	}
	return dbschema.SourceRow{Values: values}, nil
}
