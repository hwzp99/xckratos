package mongo

import (
	"context"
	"database/sql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"

	"github.com/bilibili/kratos/pkg/log"
	"github.com/pkg/errors"
)

const (
	_family          = "mongo_client"
	_slowLogDuration = time.Millisecond * 250
)

var (
	// ErrStmtNil prepared stmt error
	ErrStmtNil = errors.New("sql: prepare failed and stmt nil")
	// ErrNoMaster is returned by Master when call master multiple times.
	ErrNoMaster = errors.New("sql: no master instance")
	// ErrNoRows is returned by Scan when QueryRow doesn't return a row.
	// In such a case, QueryRow returns a placeholder *Row value that defers
	// this error until a Scan.
	ErrNoRows = sql.ErrNoRows
	// ErrTxDone transaction done.
	ErrTxDone = sql.ErrTxDone
)

type DB struct {
	MC *mongo.Client
}

// Open opens a database specified by its database driver name and a
// driver-specific data source name, usually consisting of at least a database
// name and connection information.
func Open(c *Config) (*DB, error) {
	db := new(DB)
	ctx, _ := context.WithTimeout(context.Background(), time.Duration(c.ConnTimeout))
	mc, err := mongo.Connect(ctx, options.Client().ApplyURI(c.URL))
	if err != nil {
		return nil, err
	}
	if err = mc.Ping(nil, nil); err != nil {
		return db, err
	}
	db.MC = mc
	return db, err
}

// Close closes the write and read database, releasing any open resources.
func (db *DB) Close() (err error) {
	err = db.MC.Disconnect(nil)
	return err
}

// Ping verifies a connection to the database is still alive, establishing a
// connection if necessary.
func (db *DB) Ping(c context.Context) (err error) {
	if err = db.MC.Ping(c, nil); err != nil {
		return
	}
	return
}

func slowLog(statement string, now time.Time) {
	du := time.Since(now)
	if du > _slowLogDuration {
		log.Warn("%s slow log statement: %s time: %v", _family, statement, du)
	}
}
