// Copyright (c) 2026, the openweft/weft-ha-postgresql authors
// SPDX-License-Identifier: BSD-3-Clause

package postgres

import (
	"context"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgproto3"
)

func slogDiscard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// mockQuery is a canned answer for one SQL statement: a single-column row whose
// text value is `value`, or — when `queryErr` is set — an ErrorResponse so the
// controller's error paths are exercised too.
type mockQuery struct {
	match    string // substring that identifies the statement
	colName  string
	colOID   uint32 // pg type OID (16=bool, 20=int8); pgx picks the decoder from it
	value    string
	queryErr string
}

// startMockPostgres runs an in-process, pure-Go Postgres-wire server (no initdb,
// no pg_ctl, no real Postgres) that pgx can connect to in simple-query mode. It
// answers each statement from `answers` (first substring match wins) and returns
// a DSN pointing at it. This is the "drive Postgres programmatically" answer:
// the LocalController logic is validated entirely over the wire protocol.
func startMockPostgres(t *testing.T, answers []mockQuery) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveMockConn(conn, answers)
		}
	}()

	host, port, _ := net.SplitHostPort(ln.Addr().String())
	return "postgres://tester@" + host + ":" + port +
		"/db?sslmode=disable&default_query_exec_mode=simple_protocol"
}

func serveMockConn(conn net.Conn, answers []mockQuery) {
	defer conn.Close()
	be := pgproto3.NewBackend(conn, conn)

	// Startup: pgx with sslmode=disable sends a plain StartupMessage. Accept
	// without auth and report an idle transaction.
	for {
		msg, err := be.ReceiveStartupMessage()
		if err != nil {
			return
		}
		if _, ok := msg.(*pgproto3.SSLRequest); ok {
			// Defensive: decline SSL and read the real startup message.
			_, _ = conn.Write([]byte("N"))
			continue
		}
		break
	}
	send(be, &pgproto3.AuthenticationOk{})
	send(be, &pgproto3.ParameterStatus{Name: "server_version", Value: "16.0 (weft-mock)"})
	// pgx's simple protocol requires the server to advertise these.
	send(be, &pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	send(be, &pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	send(be, &pgproto3.BackendKeyData{ProcessID: 1, SecretKey: 1})
	send(be, &pgproto3.ReadyForQuery{TxStatus: 'I'})
	if be.Flush() != nil {
		return
	}

	for {
		msg, err := be.Receive()
		if err != nil {
			return
		}
		switch m := msg.(type) {
		case *pgproto3.Query:
			answerQuery(be, m.String, answers)
		case *pgproto3.Terminate:
			return
		default:
			// Ignore anything else (e.g. Sync) to keep the mock minimal.
		}
	}
}

func answerQuery(be *pgproto3.Backend, sql string, answers []mockQuery) {
	var a *mockQuery
	for i := range answers {
		if strings.Contains(sql, answers[i].match) {
			a = &answers[i]
			break
		}
	}
	if a == nil {
		send(be, &pgproto3.ErrorResponse{Severity: "ERROR", Code: "42601", Message: "weft-mock: no canned answer for query"})
		send(be, &pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
		return
	}
	if a.queryErr != "" {
		send(be, &pgproto3.ErrorResponse{Severity: "ERROR", Code: "XX000", Message: a.queryErr})
		send(be, &pgproto3.ReadyForQuery{TxStatus: 'I'})
		_ = be.Flush()
		return
	}
	send(be, &pgproto3.RowDescription{Fields: []pgproto3.FieldDescription{{
		Name: []byte(a.colName), DataTypeOID: a.colOID, Format: 0,
	}}})
	send(be, &pgproto3.DataRow{Values: [][]byte{[]byte(a.value)}})
	send(be, &pgproto3.CommandComplete{CommandTag: []byte("SELECT 1")})
	send(be, &pgproto3.ReadyForQuery{TxStatus: 'I'})
	_ = be.Flush()
}

func send(be *pgproto3.Backend, msg pgproto3.BackendMessage) { be.Send(msg) }

func newCtx(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestLocalController_RoleOverWire(t *testing.T) {
	for _, tc := range []struct {
		name     string
		inRecov  string
		wantRole Role
	}{
		{"primary", "f", RolePrimary},
		{"replica", "t", RoleReplica},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dsn := startMockPostgres(t, []mockQuery{{match: "pg_is_in_recovery", colName: "pg_is_in_recovery", colOID: 16, value: tc.inRecov}})
			c := NewLocalController(dsn, slogDiscard())
			t.Cleanup(c.Close)
			role, err := c.Role(newCtx(t))
			if err != nil || role != tc.wantRole {
				t.Fatalf("Role = %v, %v; want %v", role, err, tc.wantRole)
			}
		})
	}
}

func TestLocalController_RoleError(t *testing.T) {
	dsn := startMockPostgres(t, []mockQuery{{match: "pg_is_in_recovery", queryErr: "boom"}})
	c := NewLocalController(dsn, slogDiscard())
	t.Cleanup(c.Close)
	if _, err := c.Role(newCtx(t)); err == nil {
		t.Fatal("expected error from failing pg_is_in_recovery")
	}
}

func TestLocalController_ReplayLSNOverWire(t *testing.T) {
	dsn := startMockPostgres(t, []mockQuery{{match: "pg_wal_lsn_diff", colName: "lsn", colOID: 20, value: "123456"}})
	c := NewLocalController(dsn, slogDiscard())
	t.Cleanup(c.Close)
	lsn, err := c.ReplayLSN(newCtx(t))
	if err != nil || lsn != 123456 {
		t.Fatalf("ReplayLSN = %d, %v; want 123456", lsn, err)
	}
}

func TestLocalController_ReplayLSNNegative(t *testing.T) {
	dsn := startMockPostgres(t, []mockQuery{{match: "pg_wal_lsn_diff", colName: "lsn", colOID: 20, value: "-1"}})
	c := NewLocalController(dsn, slogDiscard())
	t.Cleanup(c.Close)
	if _, err := c.ReplayLSN(newCtx(t)); err == nil {
		t.Fatal("expected error for negative LSN")
	}
}

func TestLocalController_PromoteOverWire(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		dsn := startMockPostgres(t, []mockQuery{{match: "pg_promote", colName: "pg_promote", colOID: 16, value: "t"}})
		c := NewLocalController(dsn, slogDiscard())
		t.Cleanup(c.Close)
		if err := c.Promote(newCtx(t)); err != nil {
			t.Fatalf("Promote: %v", err)
		}
	})
	t.Run("returns false", func(t *testing.T) {
		dsn := startMockPostgres(t, []mockQuery{{match: "pg_promote", colName: "pg_promote", colOID: 16, value: "f"}})
		c := NewLocalController(dsn, slogDiscard())
		t.Cleanup(c.Close)
		if err := c.Promote(newCtx(t)); err == nil {
			t.Fatal("expected error when pg_promote returns false")
		}
	})
}

func TestLocalController_DemoteOverWire(t *testing.T) {
	ok := []mockQuery{
		{match: "primary_conninfo", colName: "x", colOID: 25, value: "ok"},
		{match: "pg_reload_conf", colName: "pg_reload_conf", colOID: 16, value: "t"},
	}
	c := NewLocalController(startMockPostgres(t, ok), slogDiscard())
	t.Cleanup(c.Close)
	if err := c.Demote(newCtx(t), "postgres://leader:5432/db?password=o'brien"); err != nil {
		t.Fatalf("Demote: %v", err)
	}

	// ALTER SYSTEM fails.
	c2 := NewLocalController(startMockPostgres(t, []mockQuery{{match: "primary_conninfo", queryErr: "denied"}}), slogDiscard())
	t.Cleanup(c2.Close)
	if err := c2.Demote(newCtx(t), "x"); err == nil {
		t.Fatal("expected error when ALTER SYSTEM fails")
	}

	// reload fails.
	c3 := NewLocalController(startMockPostgres(t, []mockQuery{
		{match: "primary_conninfo", colName: "x", colOID: 25, value: "ok"},
		{match: "pg_reload_conf", queryErr: "reload denied"},
	}), slogDiscard())
	t.Cleanup(c3.Close)
	if err := c3.Demote(newCtx(t), "x"); err == nil {
		t.Fatal("expected error when pg_reload_conf fails")
	}
}

func TestLocalController_ConfigureSyncStandbysOverWire(t *testing.T) {
	answers := []mockQuery{
		{match: "synchronous_standby_names", colName: "x", colOID: 25, value: "ok"},
		{match: "pg_reload_conf", colName: "pg_reload_conf", colOID: 16, value: "t"},
	}
	// non-empty (ANY 1 (...)) and empty (disable) both succeed.
	for _, standbys := range [][]string{{"s1", "s2"}, {}} {
		c := NewLocalController(startMockPostgres(t, answers), slogDiscard())
		if err := c.ConfigureSyncStandbys(newCtx(t), standbys); err != nil {
			t.Fatalf("ConfigureSyncStandbys(%v): %v", standbys, err)
		}
		c.Close()
	}
	// ALTER SYSTEM fails.
	c2 := NewLocalController(startMockPostgres(t, []mockQuery{{match: "synchronous_standby_names", queryErr: "denied"}}), slogDiscard())
	t.Cleanup(c2.Close)
	if err := c2.ConfigureSyncStandbys(newCtx(t), []string{"s1"}); err == nil {
		t.Fatal("expected error when ALTER SYSTEM fails")
	}
	// reload fails.
	c3 := NewLocalController(startMockPostgres(t, []mockQuery{
		{match: "synchronous_standby_names", colName: "x", colOID: 25, value: "ok"},
		{match: "pg_reload_conf", queryErr: "reload denied"},
	}), slogDiscard())
	t.Cleanup(c3.Close)
	if err := c3.ConfigureSyncStandbys(newCtx(t), []string{"s1"}); err == nil {
		t.Fatal("expected error when pg_reload_conf fails")
	}
}

func TestLocalController_ConnectErrors(t *testing.T) {
	// A DSN pgx cannot parse makes connect() fail; every method surfaces it.
	bad := NewLocalController("postgres://u:p@host:notaport/db", slogDiscard())
	t.Cleanup(bad.Close)
	ctx := newCtx(t)
	if _, err := bad.Role(ctx); err == nil {
		t.Fatal("Role should error on bad DSN")
	}
	if _, err := bad.ReplayLSN(ctx); err == nil {
		t.Fatal("ReplayLSN should error on bad DSN")
	}
	if err := bad.Promote(ctx); err == nil {
		t.Fatal("Promote should error on bad DSN")
	}
	if err := bad.Demote(ctx, "x"); err == nil {
		t.Fatal("Demote should error on bad DSN")
	}
	if err := bad.ConfigureSyncStandbys(ctx, nil); err == nil {
		t.Fatal("ConfigureSyncStandbys should error on bad DSN")
	}
}

func TestLocalController_ConnectCachedAndClose(t *testing.T) {
	c := NewLocalController(startMockPostgres(t, []mockQuery{
		{match: "pg_is_in_recovery", colName: "pg_is_in_recovery", colOID: 16, value: "f"},
	}), slogDiscard())
	ctx := newCtx(t)
	if _, err := c.Role(ctx); err != nil { // first call creates the pool
		t.Fatalf("first Role: %v", err)
	}
	if _, err := c.Role(ctx); err != nil { // second call reuses the cached pool
		t.Fatalf("second Role: %v", err)
	}
	c.Close()
	c.Close() // idempotent
}

func TestLocalController_ReplayLSNQueryError(t *testing.T) {
	c := NewLocalController(startMockPostgres(t, []mockQuery{{match: "pg_wal_lsn_diff", queryErr: "boom"}}), slogDiscard())
	t.Cleanup(c.Close)
	if _, err := c.ReplayLSN(newCtx(t)); err == nil {
		t.Fatal("expected error from failing replay-lsn query")
	}
}

func TestLocalController_PromoteQueryError(t *testing.T) {
	c := NewLocalController(startMockPostgres(t, []mockQuery{{match: "pg_promote", queryErr: "boom"}}), slogDiscard())
	t.Cleanup(c.Close)
	if err := c.Promote(newCtx(t)); err == nil {
		t.Fatal("expected error from failing pg_promote query")
	}
}
