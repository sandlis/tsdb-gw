package graphite

import (
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/raintank/tsdb-gw/util"
)

var (
	GraphiteUrl  *url.URL
	WorldpingUrl *url.URL

	graphiteTransport http.RoundTripper
)

func Init(graphiteUrl, worldpingUrl string, graphiteDialTimeout time.Duration) error {
	var err error

	GraphiteUrl, err = url.Parse(graphiteUrl)
	if err != nil {
		return err
	}

	WorldpingUrl, err = url.Parse(worldpingUrl)
	if err != nil {
		return err
	}

	if graphiteDialTimeout == 0 {
		return errors.New("graphite-dial-timeout must be a valid non-zero duration")
	}

	graphiteTransport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   graphiteDialTimeout,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return nil
}

func Proxy(orgId int64, proxyPath string, request *http.Request) *httputil.ReverseProxy {

	// check if this is a special raintank_db requests then proxy to the worldping-api service.
	if proxyPath == "metrics/find" {
		query := request.FormValue("query")
		if strings.HasPrefix(query, "raintank_db") {
			director := func(req *http.Request) {
				req.URL.Scheme = WorldpingUrl.Scheme
				req.URL.Host = WorldpingUrl.Host
				req.URL.Path = util.JoinUrlFragments(WorldpingUrl.Path, "/api/graphite/"+proxyPath)
			}
			return &httputil.ReverseProxy{Director: director}
		}
	}

	director := func(req *http.Request) {
		req.URL.Scheme = GraphiteUrl.Scheme
		req.URL.Host = GraphiteUrl.Host
		req.Header.Del("X-Org-Id")
		req.Header.Add("X-Org-Id", strconv.FormatInt(orgId, 10))
		req.URL.Path = util.JoinUrlFragments(GraphiteUrl.Path, proxyPath)
	}

	return &httputil.ReverseProxy{
		Transport: graphiteTransport,
		Director:  director,
	}
}
