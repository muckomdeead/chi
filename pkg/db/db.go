package db

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"time"
)

// Config holds database connection pool configuration parameters.
type Config struct {
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
	CleanerInterval time.Duration
}

// DefaultConfig returns a recommended default pool configuration.
func DefaultConfig() Config {
	return Config{
		MaxOpenConns:    25,
		MaxIdleConns:    10,
		ConnMaxIdleTime: 5 * time.Minute,
		ConnMaxLifetime: 30 * time.Minute,
		CleanerInterval: 1 * time.Minute,
	}
}

// Conn represents a tracked database connection instance.
type Conn struct {
	Raw          *sql.Conn
	CreatedAt    time.Time
	LastActiveAt time.Time
	closed       bool
	mu           sync.Mutex
}

// IsExpired returns true if the connection has exceeded max lifetime or idle timeout.
func (c *Conn) IsExpired(maxIdleTime, maxLifetime time.Duration) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return true
	}

	now := time.Now()
	if maxIdleTime > 0 && !c.LastActiveAt.IsZero() && now.Sub(c.LastActiveAt) > maxIdleTime {
		return true
	}
	if maxLifetime > 0 && !c.CreatedAt.IsZero() && now.Sub(c.CreatedAt) > maxLifetime {
		return true
	}
	return false
}

// MarkActive updates the last active timestamp of the connection.
func (c *Conn) MarkActive() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.LastActiveAt = time.Now()
}

// Close closes the underlying connection cleanly.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	if c.Raw != nil {
		return c.Raw.Close()
	}
	return nil
}

// DB wraps *sql.DB and provides resilient connection management with idle eviction and retry.
type DB struct {
	*sql.DB
	cfg        Config
	conns      []*Conn
	mu         sync.Mutex
	stopCh     chan struct{}
	stopOnce   sync.Once
	cleanerRun bool
}

// Open opens a database specified by driverName and dataSourceName with the given pool configuration.
func Open(driverName, dataSourceName string, cfg Config) (*DB, error) {
	sqlDB, err := sql.Open(driverName, dataSourceName)
	if err != nil {
		return nil, err
	}
	return New(sqlDB, cfg),
		nil
}

// New creates a new DB wrapper applying the pool configuration.
func New(sqlDB *sql.DB, cfg Config) *DB {
	if cfg.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxIdleTime > 0 {
		sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}
	if cfg.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}

	db := &DB{
		DB:     sqlDB,
		cfg:    cfg,
		conns:  make([]*Conn, 0),
		stopCh: make(chan struct{}),
	}

	if cfg.CleanerInterval > 0 {
		db.startCleaner(cfg.CleanerInterval)
	}

	return db
}

// Config returns the current pool configuration.
func (db *DB) Config() Config {
	return db.cfg
}

// Acquire obtains a healthy, non-expired connection from the pool or creates a new one.
func (db *DB) Acquire(ctx context.Context) (*Conn, error) {
	for {
		var candidate *Conn

		db.mu.Lock()
		if len(db.conns) > 0 {
			candidate = db.conns[len(db.conns)-1]
			db.conns = db.conns[:len(db.conns)-1]
		}
		db.mu.Unlock()

		if candidate != nil {
			if candidate.IsExpired(db.cfg.ConnMaxIdleTime, db.cfg.ConnMaxLifetime) {
				_ = candidate.Close()
				continue
			}
			if candidate.Raw != nil {
				if err := candidate.Raw.PingContext(ctx); err != nil {
					_ = candidate.Close()
					continue
				}
			}
			candidate.MarkActive()
			return candidate, nil
		}

		rawConn, err := db.DB.Conn(ctx)
		if err != nil {
			return nil, err
		}

		now := time.Now()
		conn := &Conn{
			Raw:          rawConn,
			CreatedAt:    now,
			LastActiveAt: now,
		}
		return conn, nil
	}
}

// Release returns a connection to the pool or closes it if expired.
func (db *DB) Release(conn *Conn) {
	if conn == nil {
		return
	}
	if conn.IsExpired(db.cfg.ConnMaxIdleTime, db.cfg.ConnMaxLifetime) {
		_ = conn.Close()
		return
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	if db.cfg.MaxIdleConns > 0 && len(db.conns) >= db.cfg.MaxIdleConns {
		_ = conn.Close()
		return
	}

	conn.MarkActive()
	db.conns = append(db.conns, conn)
}

// PruneExpired evicts and closes all connections that have exceeded idle or max lifetime limits.
func (db *DB) PruneExpired() int {
	db.mu.Lock()
	var valid []*Conn
	var expired []*Conn

	for _, conn := range db.conns {
		if conn.IsExpired(db.cfg.ConnMaxIdleTime, db.cfg.ConnMaxLifetime) {
			expired = append(expired, conn)
		} else {
			valid = append(valid, conn)
		}
	}
	db.conns = valid
	db.mu.Unlock()

	for _, conn := range expired {
		_ = conn.Close()
	}
	return len(expired)
}

func (db *DB) startCleaner(interval time.Duration) {
	db.cleanerRun = true
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				db.PruneExpired()
			case <-db.stopCh:
				return
			}
		}
	}()
}

// IsBadConnError checks if an error indicates a stale or terminated connection.
func IsBadConnError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "bad connection") ||
		strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "server closed the connection") ||
		strings.Contains(msg, "invalid connection") ||
		strings.Contains(msg, "connection refused")
}

// ExecContext executes a query with a context and automatically retries once if a bad connection is encountered.
func (db *DB) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	res, err := db.DB.ExecContext(ctx, query, args...)
	if err != nil && IsBadConnError(err) {
		return db.DB.ExecContext(ctx, query, args...)
	}
	return res, err
}

// QueryContext executes a query that returns rows, retrying once on bad connection errors.
func (db *DB) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rows, err := db.DB.QueryContext(ctx, query, args...)
	if err != nil && IsBadConnError(err) {
		return db.DB.QueryContext(ctx, query, args...)
	}
	return rows, err
}

// QueryRowContext executes a query that is expected to return at most one row, retrying once on bad connection errors.
func (db *DB) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	row := db.DB.QueryRowContext(ctx, query, args...)
	if err := row.Err(); err != nil && IsBadConnError(err) {
		return db.DB.QueryRowContext(ctx, query, args...)
	}
	return row
}

// PingContext verifies connection to the database with a retry on bad connection.
func (db *DB) PingContext(ctx context.Context) error {
	err := db.DB.PingContext(ctx)
	if err != nil && IsBadConnError(err) {
		return db.DB.PingContext(ctx)
	}
	return err
}

// Close shuts down the pool cleaner and closes the underlying database.
func (db *DB) Close() error {
	db.stopOnce.Do(func() {
		close(db.stopCh)
	})
	db.mu.Lock()
	for _, conn := range db.conns {
		_ = conn.Close()
	}
	db.conns = nil
	db.mu.Unlock()
	return db.DB.Close()
}
