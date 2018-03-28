package publish

import (
	log "github.com/sirupsen/logrus"
	schema "gopkg.in/raintank/schema.v1"
)

type Publisher interface {
	Publish(metrics []*schema.MetricData) error
	Type() string
}

var (
	publisher Publisher
)

func Init(p Publisher) {
	if p == nil {
		publisher = &nullPublisher{}
	} else {
		publisher = p
	}
	log.Infof("using %s publisher", publisher.Type())
}

func Publish(metrics []*schema.MetricData) error {
	return publisher.Publish(metrics)
}

type nullPublisher struct{}

func (*nullPublisher) Publish(metrics []*schema.MetricData) error {
	log.Debugf("publishing not enabled, dropping %d metrics", len(metrics))
	return nil
}

func (*nullPublisher) Type() string {
	return "nullPublisher"
}
