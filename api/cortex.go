package api

import (
	"github.com/raintank/tsdb-gw/cortex"
)

func CortexProxy(c *Context) {
	cortex.Proxy.ServeHTTP(c.Resp, c.Req.Request)
}

func CortexWrite(c *Context) {
	cortex.WriteProxy.ServeHTTP(c.Resp, c.Req.Request)
}
