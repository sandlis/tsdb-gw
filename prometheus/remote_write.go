package prometheus

import (
	"context"
	"flag"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/raintank/metrictank/conf"
	"github.com/raintank/tsdb-gw/metric_publish"
	"github.com/raintank/tsdb-gw/metricpool"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

var (
	enabled       bool
	addr          string
	concurrency   int
	orgID         int
	schemasConf   string
	flushInterval time.Duration
	bufferSize    int

	metricPool = metricpool.NewMetricDataPool()
)

func init() {
	flag.StringVar(&addr, "prometheus-write-addr", "0.0.0.0:9201", "listen address prometheus writes")
	flag.BoolVar(&enabled, "prometheus-enabled", false, "enable prometheus input")
	flag.StringVar(&schemasConf, "prometheus-schemas-file", "/etc/storage-schemas.conf", "path to carbon storage-schemas.conf file")
	flag.IntVar(&orgID, "prometheus-org-id", 1, "Temporary flag to allow for specified org id until auth is configured")
	flag.DurationVar(&flushInterval, "prometheus-flush-interval", time.Second*5, "maximum time between flushs to kafka")
	flag.IntVar(&concurrency, "prometheus-concurrency", 1, "number of goroutines for handling metrics")
	flag.IntVar(&bufferSize, "prometheus-buffer-size", 100000, "number of metrics to hold in an input buffer. Once this buffer fills metrics will be dropped")
}

type PrometheusWriter struct {
	schemas *conf.Schemas
	reqChan chan prompb.WriteRequest
	srv     *http.Server
	flushWg sync.WaitGroup
}

func InitPrometheusWriter() *PrometheusWriter {
	p := new(PrometheusWriter)
	if !enabled {
		return p
	}
	log.Info("starting prometheus write handler")

	schemas, err := getSchemas(schemasConf)
	if err != nil {
		log.Fatal(4, "failed to load schemas config. %s", err)
	}
	p.schemas = schemas

	p.reqChan = make(chan prompb.WriteRequest, bufferSize)

	srv, err := listen(addr, p)
	if err != nil {
		log.Fatal(4, "unable to listen for prometheus metrics on %v, %v", addr, err)
	}
	p.srv = srv

	go func() {
		err := srv.ListenAndServe()
		if err != http.ErrServerClosed {
			log.Fatal(4, "prometheus write server error: %s\n", err)
		}
	}()

	for i := 0; i < concurrency; i++ {
		p.flushWg.Add(1)
		go p.flush()
	}

	log.Info("Listening for prometheus write metrics at %v/receive", addr)
	return p
}

func (p *PrometheusWriter) Stop() {
	if !enabled {
		return
	}

	log.Info("Prometheus handler server shutting down")
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := p.srv.Shutdown(ctx)
	if err != nil {
		log.Fatal(4, "Unable to shutdown prometheus write server: %v", err)
	}
	log.Info("Prometheus handler server shut down")

	close(p.reqChan)
	log.Info("Prometheus writer buffer channel closed")

	p.flushWg.Wait()
	log.Info("Prometheus write flush wait group done")
}

func (p *PrometheusWriter) flush() {
	defer p.flushWg.Done()
	buf := make([]*schema.MetricData, 0)

	ticker := time.NewTicker(flushInterval)
	for {
		select {
		case <-ticker.C:
			log.Info("prometheus writer pushed %v metrics to kafka", len(buf))
			err := metric_publish.Publish(buf)
			if err != nil {
				log.Error(3, "failed to publish metrics. %s", err)
				continue
			}
			for _, m := range buf {
				metricPool.Put(m)
			}
			buf = buf[0:0]
		case req, ok := <-p.reqChan:
			if !ok {
				return
			}
			for _, ts := range req.Timeseries {
				var name string
				var tagSet []string

				for _, l := range ts.Labels {
					if l.Name == model.MetricNameLabel {
						name = l.Value
					} else {
						tagSet = append(tagSet, l.Name+"="+l.Value)
					}
				}
				if name != "" {
					for _, sample := range ts.Samples {
						md := metricPool.Get()
						_, s := p.schemas.Match(name, 0)
						*md = schema.MetricData{
							Name:     name,
							Metric:   name,
							Interval: s.Retentions[0].SecondsPerPoint,
							Value:    sample.Value,
							Unit:     "unknown",
							Time:     (sample.Timestamp * 1000),
							Mtype:    "gauge",
							Tags:     tagSet,
							OrgId:    orgID,
						}
						md.SetId()
						buf = append(buf, md)
					}
				} else {
					log.Warn("prometheus metric received with empty name")
				}
			}
		}
	}
}

func listen(addr string, writer *PrometheusWriter) (*http.Server, error) {
	srv := &http.Server{Addr: addr, Handler: http.DefaultServeMux}
	http.HandleFunc("/receive", func(w http.ResponseWriter, r *http.Request) {
		compressed, err := ioutil.ReadAll(r.Body)
		if err != nil {
			log.Error(3, "Read Error, %v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			log.Error(3, "Decode Error, %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			log.Error(3, "Unmarshal Error, %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		writer.reqChan <- req
	})

	return srv, nil
}

func getSchemas(file string) (*conf.Schemas, error) {
	schemas, err := conf.ReadSchemas(file)
	if err != nil {
		return nil, err
	}
	return &schemas, nil
}
