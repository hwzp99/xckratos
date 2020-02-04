package mongo

import (
	"github.com/bilibili/kratos/pkg/log"
	"github.com/bilibili/kratos/pkg/net/netutil/breaker"
	"github.com/bilibili/kratos/pkg/time"
)

// Config url config.
type Config struct {
	URL          string          // mongourl.
	ConnTimeout  time.Duration   // connect max life time.
	QueryTimeout time.Duration   // query timeout
	ExecTimeout  time.Duration   // execute timeout
	Breaker      *breaker.Config // breaker
}

// NewMongo new db and retry connection when has error.
func NewMongo(c *Config) (db *DB) {
	if c.QueryTimeout == 0 || c.ExecTimeout == 0 {
		panic("mongo must be set query/execute timeout")
	}
	db, err := Open(c)
	if err != nil {
		log.Error("open mongo error(%v)", err)
		panic(err)
	}
	return
}
