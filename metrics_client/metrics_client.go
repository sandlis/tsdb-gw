package metrics_client

import (
	"bytes"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/golang/snappy"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	schema "gopkg.in/raintank/schema.v1"
	"gopkg.in/raintank/schema.v1/msg"
)

type Config struct {
	Addr   string
	APIKey string
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	flag.StringVar(&cfg.Addr, "gateway-addr", "localhost:80/metrics", "address of the gateway to persist metrics to.")
	flag.StringVar(&cfg.APIKey, "gateway-key", "not_very_secret_key", "api key to use when pushing metrics to the gateway")
}

type Client struct {
	gwURL        *url.URL
	gwAPIKey     string
	client       *http.Client
	sendDuration *prometheus.HistogramVec
}

func New(cfg Config) (*Client, error) {
	u, err := url.Parse(cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("unable to parse gw.addr: '%v'", cfg.Addr)
	}

	return &Client{
		gwURL:    u,
		gwAPIKey: cfg.APIKey,
		client:   &http.Client{},
		sendDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "persister",
			Name:      "gw_send_duration_seconds",
			Help:      "Time spent sending a sample batch to multiple replicated ingesters.",
			Buckets:   []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}, []string{"status_code"}),
	}, nil
}

func (c *Client) Push(metrics []*schema.MetricData) error {
	now := time.Now()

	statusCode, err := c.push(metrics)
	if err != nil {
		log.Errorf("unable to send %v metrics, err: %v", len(metrics), err)
		return err
	}

	took := time.Since(now)
	status := strconv.Itoa(statusCode)
	c.sendDuration.WithLabelValues(status).Observe(took.Seconds())

	return nil
}

func (c *Client) push(metrics []*schema.MetricData) (int, error) {
	if len(metrics) < 1 {
		return 0, fmt.Errorf("no metrics to publish")
	}

	orgID := strconv.Itoa(metrics[0].OrgId)
	mda := schema.MetricDataArray(metrics)
	data, err := msg.CreateMsg(mda, 0, msg.FormatMetricDataArrayMsgp)
	if err != nil {
		return 0, err
	}

	body := new(bytes.Buffer)
	snappyBody := snappy.NewWriter(body)
	snappyBody.Write(data)
	snappyBody.Close()
	req, err := http.NewRequest("POST", c.gwURL.String(), body)
	if err != nil {
		log.Errorf("unable to publish metrics: %v", err)
		return 0, err
	}
	req.SetBasicAuth(orgID, c.gwAPIKey)
	req.Header.Add("Content-Type", "rt-metric-binary-snappy")
	resp, err := c.client.Do(req)

	if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
		ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		return resp.StatusCode, nil
	}

	if err != nil {
		log.Warningf("failed to submit data: %s", err)
		return 0, err
	}

	buf := make([]byte, 300)
	n, _ := resp.Body.Read(buf)
	log.Warningf("failed to persist data: http %d - %s", resp.StatusCode, buf[:n])

	ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	return resp.StatusCode, nil
}
