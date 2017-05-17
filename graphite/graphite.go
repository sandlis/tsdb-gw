package graphite

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"

	"github.com/raintank/tsdb-gw/util"
	"gopkg.in/macaron.v1"
)

var (
	GraphiteUrl  *url.URL
	WorldpingUrl *url.URL
	wpProxy      httputil.ReverseProxy
	gProxy       httputil.ReverseProxy
)

func Init(graphiteUrl, worldpingUrl string) error {
	var err error
	GraphiteUrl, err = url.Parse(graphiteUrl)
	if err != nil {
		return err
	}
	WorldpingUrl, err = url.Parse(worldpingUrl)
	if err != nil {
		return err
	}

	wpProxy.Director = func(req *http.Request) {
		req.URL.Scheme = WorldpingUrl.Scheme
		req.URL.Host = WorldpingUrl.Host
	}

	gProxy.Director = func(req *http.Request) {
		req.URL.Scheme = GraphiteUrl.Scheme
		req.URL.Host = GraphiteUrl.Host
	}

	return nil
}

func Proxy(orgId int64, c *macaron.Context) {
	proxyPath := c.Params("*")

	// check if this is a special raintank_db c.Req.Requests then proxy to the worldping-api service.
	if proxyPath == "metrics/find" {
		query := c.Req.Request.FormValue("query")
		if strings.HasPrefix(query, "raintank_db") {
			c.Req.Request.URL.Path = util.JoinUrlFragments(WorldpingUrl.Path, "/api/graphite/"+proxyPath)
			wpProxy.ServeHTTP(c.Resp, c.Req.Request)
			return
		}
	}

	c.Req.Request.Header.Del("X-Org-Id")
	c.Req.Request.Header.Add("X-Org-Id", strconv.FormatInt(orgId, 10))
	c.Req.Request.URL.Path = util.JoinUrlFragments(GraphiteUrl.Path, proxyPath)
	gProxy.ServeHTTP(c.Resp, c.Req.Request)
}
