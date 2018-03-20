package cortex

import (
	"flag"
	"net/http"
	"net/http/httputil"
	"net/url"
)

var (
	cortexWriteURL = flag.String("cortex-write-url", "http://localhost:9000", "cortex write address")
	cortexReadURL  = flag.String("cortex-read-url", "http://localhost:9000", "cortex read address")

	// Proxy handles all non write requests to cortex
	Proxy *httputil.ReverseProxy

	// WriteProxy handles all write requests to cortex
	WriteProxy *httputil.ReverseProxy
)

// Init initializes the cortex reverse proxies
func Init() error {

	CortexReadURL, err := url.Parse(*cortexReadURL)
	if err != nil {
		return err
	}
	Proxy = httputil.NewSingleHostReverseProxy(CortexReadURL)

	CortexWriteURL, err := url.Parse(*cortexWriteURL)
	if err != nil {
		return err
	}
	// Seperate Proxy for Writes, add BufferPool for perf reasons if needed
	WriteProxy = &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = CortexWriteURL.Scheme
			req.URL.Host = CortexWriteURL.Host
		},
		// BufferPool: (add BufferPool for perf reasons if needed)
	}
	return nil
}
