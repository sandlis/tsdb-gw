package api

import (
	"github.com/raintank/tsdb-gw/graphite"
)

func GraphiteProxy(c *Context) {
	proxyPath := c.Params("*")
	proxy := graphite.Proxy(c.OrgId, proxyPath, c.Req.Request)
	proxy.ServeHTTP(c.Resp, c.Req.Request)
}
