package persist

import (
	"github.com/raintank/tsdb-gw/metrics_client"
	"github.com/sirupsen/logrus"
	schema "gopkg.in/raintank/schema.v1"
)

// The persist package contains client to push metrics to a persister service
var (
	client  *metrics_client.Client
	enabled = false
)

func Init(addr, apiKey string) {
	cli, err := metrics_client.New(metrics_client.Config{
		Addr:   addr,
		APIKey: apiKey,
	})

	if err != nil {
		logrus.Fatalf("unable to initialize peristor: %v", err)
	}

	client = cli
	enabled = true
}

func Persist(metrics []*schema.MetricData) error {
	if enabled {
		return client.Push(metrics)
	}

	logrus.Infof("persist not enabled, dropping %v metrics", len(metrics))
	return nil
}
