package cortex

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/oxtoacart/bpool"
	"github.com/prometheus/prometheus/prompb"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context/ctxhttp"
	schema "gopkg.in/raintank/schema.v1"
)

var (
	cortexWriteBPoolSize  = flag.Int("cortex-bpool-size", 100, "max number of byte buffers in the cortex write buffer pool")
	cortexWriteBPoolWidth = flag.Int("cortex-bpool-width", 1024, "capacity of byte array provided by cortex write buffer pool")
	cortexWriteURL        = flag.String("cortex-write-url", "http://localhost:9000", "cortex write address")

	writeProxy *httputil.ReverseProxy

	errBadTag = errors.New("unable to parse tags")
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

	return &cortexPublisher{
		url:     CortexWriteURL,
		client:  httpClient,
		timeout: time.Second * 60,
	}
}

func (c *cortexPublisher) Publish(metrics []*schema.MetricData) error {

	series := []*prompb.TimeSeries{}
	for _, metric := range metrics {
		ts, err := packageMetric(metric)
		if err != nil {
			log.Debugf("unable to package metric '%v', %v", metric, err)
		}
		series = append(series, ts)
	}
	return c.Write(&prompb.WriteRequest{
		Timeseries: series,
	})
}

func (c *cortexPublisher) Type() string {
	return "cortex"
}

// Store sends a batch of samples to the HTTP endpoint.
func (c *cortexPublisher) Write(req *prompb.WriteRequest) error {
	data, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	compressed := snappy.Encode(nil, data)
	httpReq, err := http.NewRequest("POST", c.url.String(), bytes.NewReader(compressed))
	if err != nil {
		// Errors from NewRequest are from unparseable URLs, so are not
		// recoverable.
		return err
	}
	httpReq.Header.Add("Content-Encoding", "snappy")
	httpReq.Header.Set("Content-Type", "application/x-protobuf")
	httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")

	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	httpResp, err := ctxhttp.Do(ctx, c.client, httpReq)
	if err != nil {
		// Errors from client.Do are from (for example) network errors, so are
		// recoverable.
		return err
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode/100 != 2 {
		scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, maxErrMsgLen))
		line := ""
		if scanner.Scan() {
			line = scanner.Text()
		}
		err = fmt.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
	}
	if httpResp.StatusCode/100 == 5 {
		return err
	}
	return err
}

func packageMetric(metric *schema.MetricData) (*prompb.TimeSeries, error) {
	labels := []*prompb.Label{}
	labels = append(labels, &prompb.Label{
		Name:  "__name__",
		Value: slugifyName(metric.Name),
	})

	for _, tag := range metric.Tags {
		m := strings.SplitN(tag, "=", 2)
		if len(m) < 2 {
			return nil, errBadTag
		}
		labels = append(labels, &prompb.Label{
			Name:  m[0],
			Value: m[1],
		})
	}

	return &prompb.TimeSeries{
		Labels: labels,
		Samples: []*prompb.Sample{
			&prompb.Sample{
				Value:     metric.Value,
				Timestamp: metric.Time * 1000,
			},
		},
	}, nil
}

func slugifyName(name string) string {
	return strings.Replace(name, ".", "_", -1)
}
