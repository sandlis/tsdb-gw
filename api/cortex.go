package api

import (
	"github.com/raintank/tsdb-gw/cortex"
	"strconv"
)

func CortexProxy(c *Context) {
	c.Req.Request.Header.Set("X-Scope-OrgID", strconv.Itoa(c.User.ID))
	cortex.Proxy.ServeHTTP(c.Resp, c.Req.Request)
}

func CortexWrite(c *Context) {
	c.Req.Request.Header.Set("X-Scope-OrgID", strconv.Itoa(c.User.ID))
	cortex.WriteProxy.ServeHTTP(c.Resp, c.Req.Request)
}
