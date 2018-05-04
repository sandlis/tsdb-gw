package cortex

import (
	"flag"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/raintank/tsdb-gw/api/models"
	log "github.com/sirupsen/logrus"
)

var (
	readURL = flag.String("read-url", "http://localhost:9000", "cortex read address")

	// Proxy handles all non write requests to cortex
	proxy *httputil.ReverseProxy
)

// Init initializes the cortex reverse proxies
func Init() error {

	cortexURL, err := url.Parse(*readURL)
	if err != nil {
		return err
	}
	proxy = httputil.NewSingleHostReverseProxy(cortexURL)
	log.Infof("cortex read proxy intitialized, backend=%v", cortexURL)

	return nil
}

func Proxy(c *models.Context) {
	c.Req.Request.Header.Set("X-Scope-OrgID", strconv.Itoa(c.User.ID))
	proxy.ServeHTTP(c.Resp, c.Req.Request)
}
