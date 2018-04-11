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
	"github.com/prometheus/client_golang/prometheus"
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

	droppedSamplesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "cortex_gw",
			Subsystem: "publisher",
			Name:      "dropped_samples_total",
			Help:      "Total number of samples which were dropped.",
		},
		[]string{},
	)
	succeededSamplesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "cortex_gw",
			Subsystem: "publisher",
			Name:      "succeeded_samples_total",
			Help:      "Total number of samples successfully sent.",
		},
		[]string{},
	)
	requestDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "cortex_gw",
		Subsystem: "publisher",
		Name:      "publish_duration_seconds",
		Help:      "Time (in seconds) spent publishing metrics to cortex.",
		Buckets:   prometheus.ExponentialBuckets(.05, 2, 10),
	}, []string{"status"})
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

	prometheus.MustRegister(droppedSamplesTotal)
	prometheus.MustRegister(succeededSamplesTotal)
	prometheus.MustRegister(requestDuration)
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
	start := time.Now()
	req, err := packageMetrics(metrics)
	if err != nil {
		log.Debugf("unable to package metrics, %v", err)
		droppedSamplesTotal.WithLabelValues().Add(float64(len(metrics)))
		return err
	}

	succeededSamplesTotal.WithLabelValues().Add(float64(len(metrics)))

	err = c.Write(req)
	took := time.Since(start)
	if err != nil {
		requestDuration.WithLabelValues("failed").Observe(took.Seconds())
		return err
	}

	requestDuration.WithLabelValues("succeeded").Observe(took.Seconds())
	return nil
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

func packageMetrics(metrics []*schema.MetricData) (*prompb.WriteRequest, error) {
	req := &prompb.WriteRequest{
		Timeseries: make([]*prompb.TimeSeries, 0, len(metrics)),
	}
	for _, m := range metrics {
		labels := make([]*prompb.Label, 0, len(m.Tags)+1)
		labels = append(labels,
			&prompb.Label{
				Name:  "__name__",
				Value: strings.Replace(m.Name, ".", "_", -1),
			},
		)
		for _, tag := range m.Tags {
			tv := strings.SplitN(tag, "=", 2)
			if len(tv) < 2 || tv[1] == "" {
				return nil, errBadTag
			}
			labels = append(labels, &prompb.Label{
				Name:  tv[0],
				Value: tv[1],
			})
		}
		req.Timeseries = append(req.Timeseries, &prompb.TimeSeries{
			Labels: labels,
			Samples: []*prompb.Sample{
				{
					Value:     m.Value,
					Timestamp: m.Time * 1000,
				},
			},
		})
	}

	return req, nil
}
