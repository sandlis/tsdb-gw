package metrictank

import (
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"github.com/raintank/tsdb-gw/util"
)

var (
	MetrictankUrl *url.URL

	transport http.RoundTripper
)

func Init(metrictankUrl string, dialTimeout time.Duration) error {
	var err error
	MetrictankUrl, err = url.Parse(metrictankUrl)
	if err != nil {
		return err
	}

	if dialTimeout == 0 {
		return errors.New("metrictank-dial-timeout must be a valid non-zero duration")
	}

	transport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   dialTimeout,
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

func ProxyDelete(orgId int64) *httputil.ReverseProxy {
	director := func(req *http.Request) {
		req.URL.Scheme = MetrictankUrl.Scheme
		req.URL.Host = MetrictankUrl.Host
		req.URL.Path = util.JoinUrlFragments(MetrictankUrl.Path, "/metrics/delete")
		req.Header.Del("X-Org-Id")
		req.Header.Add("X-Org-Id", strconv.FormatInt(orgId, 10))
	}
	return &httputil.ReverseProxy{
		Transport: transport,
		Director:  director,
	}
}
