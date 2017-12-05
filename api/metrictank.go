package api

import (
	"github.com/raintank/tsdb-gw/metrictank"
)

func MetrictankProxy(path string) func(c *Context) {
	return func(c *Context) {
		proxy := metrictank.Proxy(c.OrgId, path)
		proxy.ServeHTTP(c.Resp, c.Req.Request)
	}
}
