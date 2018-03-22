package api

import (
	"github.com/raintank/tsdb-gw/graphite"
)

func GraphiteProxy(c *Context) {
	graphite.Proxy(c.ID, c.Context)
}
