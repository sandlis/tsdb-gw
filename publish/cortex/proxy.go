package cortex

import (
	"strconv"

	"github.com/raintank/tsdb-gw/api"
)

func Write(c *api.Context) {
	c.Req.Request.Header.Set("X-Scope-OrgID", strconv.Itoa(c.User.ID))
	writeProxy.ServeHTTP(c.Resp, c.Req.Request)
}
