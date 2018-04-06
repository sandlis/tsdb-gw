package metrictank

import (
	"github.com/raintank/tsdb-gw/api"
)

func PrometheusProxy(c *api.Context) {
	proxyPath := c.Params("*")
	proxy := Proxy(c.ID, "/prometheus/"+proxyPath)
	proxy.ServeHTTP(c.Resp, c.Req.Request)
}
