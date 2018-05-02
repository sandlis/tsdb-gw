package persister

import (
	"context"
	"encoding/json"
	"flag"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/raintank/tsdb-gw/metrics_client"
	"github.com/raintank/tsdb-gw/persister/storage"
	"github.com/raintank/tsdb-gw/persister/storage/gcp"
	log "github.com/sirupsen/logrus"
	schema "gopkg.in/raintank/schema.v1"
	"gopkg.in/raintank/schema.v1/msg"
)

type Persister struct {
	*sync.Mutex
	metrics  []*schema.MetricData
	client   *metrics_client.Client
	store    storage.Storage
	interval int
	orgID    int
}

type Config struct {
	orgID               int
	interval            int
	MetricsClientConfig metrics_client.Config
	StorageClientConfig gcp.Config
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.IntVar(&cfg.orgID, "orgID", 1, "org id for the persister to manage")
	f.IntVar(&cfg.interval, "persister-interval", 60, "seconds between sending persisted metrics")
	cfg.MetricsClientConfig.RegisterFlags(f)
	cfg.StorageClientConfig.RegisterFlags(f)
}

func NewPersister(cfg *Config) (*Persister, error) {
	client, err := metrics_client.New(cfg.MetricsClientConfig)
	if err != nil {
		return nil, err
	}

	store, err := gcp.NewStorageClient(context.Background(), cfg.StorageClientConfig)
	if err != nil {
		return nil, err
	}

	metrics, err := store.Retrieve(cfg.orgID)

	if err != nil {
		return nil, err
	}

	log.Infof("loaded %v metrics to persist from storage", len(metrics))
	return &Persister{
		&sync.Mutex{},
		metrics,
		client,
		store,
		cfg.interval,
		cfg.orgID,
	}, nil
}

func (p *Persister) PersistHandler(w http.ResponseWriter, r *http.Request) {
	body := ioutil.NopCloser(snappy.NewReader(r.Body))
	defer body.Close()

	if r.Body != nil {
		data, err := ioutil.ReadAll(body)
		if err != nil {
			log.Errorf("unable to read request body. %s", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		metricData := new(msg.MetricData)
		err = metricData.InitFromMsg(data)
		if err != nil {
			log.Errorf("payload not metricData. %s", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = metricData.DecodeMetricData()
		if err != nil {
			log.Errorf("failed to unmarshal metricData. %s", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		err = p.Persist(metricData.Metrics)
		if err != nil {
			log.Errorf("failed to persist metricData. %s", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Write([]byte("ok"))
		return
	}
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte("no metrics to persists"))
}

func (p *Persister) RemoveHandler(w http.ResponseWriter, r *http.Request) {
	if r.Body != nil {
		body, err := ioutil.ReadAll(r.Body)

		if err != nil {
			log.Errorf("unable to read request body. %s", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		metrics := make([]*schema.MetricData, 0)
		err = json.Unmarshal(body, &metrics)

		log.Infof("removing %v metrics", len(metrics))
		err = p.store.Remove(metrics)
		if err != nil {
			log.Errorf("failed to remove metrics: %s", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		p.Lock()
		log.Infof("reloading metrics from store")
		p.metrics, err = p.store.Retrieve(p.orgID)
		p.Unlock()

		if err != nil {
			log.Errorf("failed to remove metrics: %s", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Write([]byte("ok"))
		return
	}

	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte("no metrics to remove"))
}

func (p *Persister) IndexHandler(w http.ResponseWriter, r *http.Request) {
	data, err := json.Marshal(p.metrics)
	if err != nil {
		log.Errorf("unable to marshal metrics index: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.Write(data)
	return
}

func (p *Persister) Persist(metrics []*schema.MetricData) error {
	log.Infof("persisting %v metrics", len(metrics))
	p.Lock()
	err := p.store.Store(metrics)
	if err != nil {
		log.Error(err)
		return err
	}
	p.metrics = append(p.metrics, metrics...)
	p.Unlock()
	return nil
}

func (p *Persister) Push(quit chan struct{}) {
	ticker := time.NewTicker(time.Duration(p.interval) * time.Second)
	for {
		select {
		case <-ticker.C:
			now := time.Now().Unix()
			p.Lock()
			for _, metric := range p.metrics {
				metric.Time = now
				metric.Interval = p.interval
			}
			if len(p.metrics) > 0 {
				err := p.client.Push(p.metrics)
				if err != nil {
					log.Errorf("unable to publish: %v", err)
				}
			}
			p.Unlock()
		case <-quit:
			ticker.Stop()
			return
		}
	}
}
