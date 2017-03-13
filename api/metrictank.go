package api

import (
	"github.com/raintank/tsdb-gw/metrictank"
)

func MetrictankProxy(c *Context) {
	// currently the only action on metrictank is delete
	proxy := metrictank.ProxyDelete(c.OrgId)
	proxy.ServeHTTP(c.Resp, c.Req.Request)
}
