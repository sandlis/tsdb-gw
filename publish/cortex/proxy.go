package cortex

import (
	"strconv"

	"github.com/raintank/tsdb-gw/api/models"
)

// Write adds the required headers and forwards the write request.
func Write(c *models.Context) {
	c.Req.Request.Header.Set("X-Scope-OrgID", strconv.Itoa(c.User.ID))
	writeProxy.ServeHTTP(c.Resp, c.Req.Request)
}
