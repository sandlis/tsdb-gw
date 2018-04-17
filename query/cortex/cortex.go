package cortex

import (
	"flag"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/raintank/tsdb-gw/api"
)

var (
	cortexReadURL = flag.String("cortex-read-url", "http://localhost:9000", "cortex read address")

	// Proxy handles all non write requests to cortex
	proxy *httputil.ReverseProxy
)

// Init initializes the cortex reverse proxies
func Init() error {

	CortexReadURL, err := url.Parse(*cortexReadURL)
	if err != nil {
		return err
	}
	proxy = httputil.NewSingleHostReverseProxy(CortexReadURL)

	return nil
}

func Proxy(c *api.Context) {
	c.Req.Request.Header.Set("X-Scope-OrgID", strconv.Itoa(c.User.ID))
	proxy.ServeHTTP(c.Resp, c.Req.Request)
}
