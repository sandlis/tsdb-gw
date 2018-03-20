package cortex

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"

	"github.com/raintank/tsdb-gw/util"
)

var (
	CortexUrl  *url.URL
	WriteProxy *httputil.ReverseProxy
)

func Init(cortexUrl string) error {
	var err error
	CortexUrl, err = url.Parse(cortexUrl)
	if err != nil {
		return err
	}
	// Seperate Proxy for Writes, add BufferPool for perf reasons if needed
	WriteProxy = &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = CortexUrl.Scheme
			req.URL.Host = CortexUrl.Host
			req.URL.Path = "/api/prom/push"
		},
		// BufferPool: (add BufferPool for perf reasons if needed)
	}
	return nil
}

// Proxy Other cortex requests
func Proxy(orgId int, path string) *httputil.ReverseProxy {
	director := func(req *http.Request) {
		req.URL.Scheme = CortexUrl.Scheme
		req.URL.Host = CortexUrl.Host
		req.URL.Path = util.JoinUrlFragments(CortexUrl.Path, path)
		req.Header.Del("X-Org-Id")
		req.Header.Add("X-Scope-OrgID", strconv.FormatInt(int64(orgId), 10))
	}
	return &httputil.ReverseProxy{Director: director}
}
