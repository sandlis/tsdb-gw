package cortex

import (
	"flag"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"time"

	"github.com/oxtoacart/bpool"
	"github.com/prometheus/common/config"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/raintank/tsdb-gw/api"
	schema "gopkg.in/raintank/schema.v1"
)

var (
	cortexWriteURL = flag.String("cortex-write-url", "http://localhost:9000", "cortex write address")

	cortexWriteBPoolSize  = flag.Int("cortex-bpool-size", 100, "max number of byte buffers in the cortex write buffer pool")
	cortexWriteBPoolWidth = flag.Int("cortex-bpool-width", 1024, "capacity of byte array provided by cortex write buffer pool")

	// WriteProxy handles all write requests to cortex
	writeProxy *httputil.ReverseProxy
)

const maxErrMsgLen = 256

// Init initializes the cortex reverse proxies
func init() {
	CortexWriteURL, err := url.Parse(*cortexWriteURL)
	if err != nil {
		log.Fatalf("unable to parse cortex write url '%s': %v", *cortexWriteURL, err)
	}
	// Seperate Proxy for Writes, add BufferPool for perf reasons if needed
	writeProxy = &httputil.ReverseProxy{
		Director: func(req *http.Request) {
			req.URL.Scheme = CortexWriteURL.Scheme
			req.URL.Host = CortexWriteURL.Host
		},
		BufferPool: bpool.NewBytePool(*cortexWriteBPoolSize, *cortexWriteBPoolWidth),
	}
}

type cortexPublisher struct {
	url     *url.URL
	client  *http.Client
	timeout time.Duration
}

func NewCortexPublisher() *cortexPublisher {
	CortexWriteURL, err := url.Parse(*cortexWriteURL)
	if err != nil {
		log.Fatalf("unable to parse cortex write url '%v': %v", *cortexWriteURL, err)
	}

	httpClient := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        20000,
			MaxIdleConnsPerHost: 1000,
			DisableKeepAlives:   false,
			DisableCompression:  true,
			IdleConnTimeout:     5 * time.Minute,
		},
	}

	remote.NewClient(0, &remote.ClientConfig{
		URL: &config.URL{
			CortexWriteURL,
		},
	})

	return &cortexPublisher{
		url:     CortexWriteURL,
		client:  httpClient,
		timeout: time.Second * 60,
	}
}

func (c *cortexPublisher) Publish(metrics []*schema.MetricData) error {
	return nil
}

func (c *cortexPublisher) Type() string {
	return "cortex"
}

func Write(c *api.Context) {
	c.Req.Request.Header.Set("X-Scope-OrgID", strconv.Itoa(c.User.ID))
	writeProxy.ServeHTTP(c.Resp, c.Req.Request)
}

// // Store sends a batch of samples to the HTTP endpoint.
// func (c *cortexPublisher) Write(req *prompb.WriteRequest) error {
// 	data, err := proto.Marshal(req)
// 	if err != nil {
// 		return err
// 	}

// 	compressed := snappy.Encode(nil, data)
// 	httpReq, err := http.NewRequest("POST", c.url.String(), bytes.NewReader(compressed))
// 	if err != nil {
// 		// Errors from NewRequest are from unparseable URLs, so are not
// 		// recoverable.
// 		return err
// 	}
// 	httpReq.Header.Add("Content-Encoding", "snappy")
// 	httpReq.Header.Set("Content-Type", "application/x-protobuf")
// 	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

// 	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
// 	defer cancel()

// 	httpResp, err := ctxhttp.Do(ctx, c.client, httpReq)
// 	if err != nil {
// 		// Errors from client.Do are from (for example) network errors, so are
// 		// recoverable.
// 		return err
// 	}
// 	defer httpResp.Body.Close()

// 	if httpResp.StatusCode/100 != 2 {
// 		scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, maxErrMsgLen))
// 		line := ""
// 		if scanner.Scan() {
// 			line = scanner.Text()
// 		}
// 		err = fmt.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
// 	}
// 	if httpResp.StatusCode/100 == 5 {
// 		return err
// 	}
// 	return err
// }
