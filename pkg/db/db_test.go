package db

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestIsBadConnError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{"nil error", nil, false},
		{"driver.ErrBadConn", driver.ErrBadConn, true},
		{"io.EOF", io.EOF, true},
		{"net.ErrClosed", net.ErrClosed, true},
		{"connection reset", errors.New("read: connection reset by peer"), true},
		{"broken pipe", errors.New("write: broken pipe"), true},
		{"bad connection text", errors.New("driver: bad connection"), true},
		{"regular error", errors.New("syntax error near SELECT"), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsBadConnError(tt.err)
			if result != tt.expected {
				t.Errorf("IsBadConnError(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}

func TestConnIsExpired(t *testing.T) {
	conn := &Conn{
		CreatedAt:    time.Now().Add(-10 * time.Minute),
		LastActiveAt: time.Now().Add(-2 * time.Minute),
	}

	// Not expired with long timeouts
	if conn.IsExpired(5*time.Minute, 30*time.Minute) {
		t.Errorf("expected connection to not be expired")
	}

	// Expired by idle timeout
	if !conn.IsExpired(1*time.Minute, 30*time.Minute) {
		t.Errorf("expected connection to be expired by idle timeout")
	}

	// Expired by lifetime
	if !conn.IsExpired(5*time.Minute, 5*time.Minute) {
		t.Errorf("expected connection to be expired by max lifetime")
	}

	// Closed connection is always expired
	_ = conn.Close()
	if !conn.IsExpired(10*time.Hour, 10*time.Hour) {
		t.Errorf("expected closed connection to be expired")
	}
}

func TestPruneExpired(t *testing.T) {
	db := &DB{
		cfg: Config{
			ConnMaxIdleTime: 50 * time.Millisecond,
			ConnMaxLifetime: 500 * time.Millisecond,
		},
		stopCh: make(chan struct{}),
	}

	activeConn := &Conn{
		CreatedAt:    time.Now(),
		LastActiveAt: time.Now(),
	}
	staleConn := &Conn{
		CreatedAt:    time.Now().Add(-1 * time.Second),
		LastActiveAt: time.Now().Add(-100 * time.Millisecond),
	}

	db.conns = []*Conn{activeConn, staleConn}

	pruned := db.PruneExpired()
	if pruned != 1 {
		t.Errorf("expected 1 pruned connection, got %d", pruned)
	}
	if len(db.conns) != 1 {
		t.Errorf("expected 1 remaining connection, got %d", len(db.conns))
	}
	if db.conns[0] != activeConn {
		t.Errorf("expected remaining connection to be activeConn")
	}
}

func TestReleaseAndAcquireWithIdleTimeout(t *testing.T) {
	db := &DB{
		cfg: Config{
			ConnMaxIdleTime: 20 * time.Millisecond,
			ConnMaxLifetime: 1 * time.Minute,
			MaxIdleConns:    5,
		},
		stopCh: make(chan struct{}),
	}

	conn := &Conn{
		CreatedAt:    time.Now(),
		LastActiveAt: time.Now(),
	}
	db.Release(conn)

	if len(db.conns) != 1 {
		t.Fatalf("expected 1 connection in pool, got %d", len(db.conns))
	}

	time.Sleep(30 * time.Millisecond)

	pruned := db.PruneExpired()
	if pruned != 1 {
		t.Errorf("expected 1 connection pruned after sleep, got %d", pruned)
	}
	if len(db.conns) != 0 {
		t.Errorf("expected pool to be empty after prune, got %d", len(db.conns))
	}
}

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.MaxOpenConns <= 0 || cfg.MaxIdleConns <= 0 || cfg.ConnMaxIdleTime <= 0 || cfg.ConnMaxLifetime <= 0 {
		t.Errorf("invalid default config values: %+v", cfg)
	}
	if cfg.ConnMaxIdleTime >= cfg.ConnMaxLifetime {
		t.Errorf("idle timeout should be less than max lifetime")
	}
}
