package api

import (
	"strconv"

	"github.com/raintank/tsdb-gw/cortex"
)

func CortexProxy(c *Context) {
	proxyPath := c.Params("*")
	proxy := cortex.Proxy(c.OrgId, proxyPath)
	proxy.ServeHTTP(c.Resp, c.Req.Request)
}

func CortexWrite(c *Context) {
	c.Req.Request.Header.Add("X-Scope-OrgID", strconv.FormatInt(int64(c.OrgId), 10))
	cortex.WriteProxy.ServeHTTP(c.Resp, c.Req.Request)
}
