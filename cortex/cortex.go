package cortex

import (
	"flag"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/oxtoacart/bpool"
)

var (
	cortexWriteURL = flag.String("cortex-write-url", "http://localhost:9000", "cortex write address")
	cortexReadURL  = flag.String("cortex-read-url", "http://localhost:9000", "cortex read address")

	cortexWriteBPoolSize  = flag.Int("cortex-bpool-size", 100, "max number of byte buffers in the cortex write buffer pool")
	cortexWriteBPoolWidth = flag.Int("cortex-bpool-width", 1024, "capacity of byte array provided by cortex write buffer pool")

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
		BufferPool: bpool.NewBytePool(*cortexWriteBPoolSize, *cortexWriteBPoolWidth),
	}
	return nil
}
