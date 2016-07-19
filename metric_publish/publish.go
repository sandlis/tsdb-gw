package metric_publish

import (
	"encoding/binary"
	"time"

	"github.com/Shopify/sarama"
	"github.com/raintank/met"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v0"
)

var (
	config            *sarama.Config
	producer          sarama.SyncProducer
	topic             string
	brokers           []string
	metricsPublished  met.Count
	messagesPublished met.Count
	messagesSize      met.Meter
	metricsPerMessage met.Meter
	publishDuration   met.Timer
)

func getCompression(codec string) sarama.CompressionCodec {
	switch codec {
	case "none":
		return sarama.CompressionNone
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	default:
		log.Fatal(5, "unknown compression codec %q", codec)
		return 0 // make go compiler happy, needs a return *roll eyes*
	}
}

func Init(metrics met.Backend, t, broker, codec string, enabled bool) {
	if !enabled {
		return
	}
	// We are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = getCompression(codec)
	err := config.Validate()
	if err != nil {
		log.Fatal(4, "failed to validate kafka config. %s", err)
	}

	topic = t
	brokers = []string{broker}

	producer, err = sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatal(4, "failed to initialize kafka producer. %s", err)
	}
	metricsPublished = metrics.NewCount("metricpublisher.metrics-published")
	messagesPublished = metrics.NewCount("metricpublisher.messages-published")
	messagesSize = metrics.NewMeter("metricpublisher.message_size", 0)
	metricsPerMessage = metrics.NewMeter("metricpublisher.metrics_per_message", 0)
	publishDuration = metrics.NewTimer("metricpublisher.publish_duration", 0)
}

func Publish(metrics []*schema.MetricData) error {
	if producer == nil {
		log.Debug("droping %d metrics as publishing is disabled", len(metrics))
		return nil
	}
	if len(metrics) == 0 {
		return nil
	}
	var data []byte

	payload := make([]*sarama.ProducerMessage, len(metrics))
	pre := time.Now()

	for i, metric := range metrics {
		data, err := metric.MarshalMsg(data[:])
		if err != nil {
			return err
		}

		// partition by organisation: metrics for the same org should go to the same
		// partition/MetricTank (optimize for locality~performance)
		// the extra 4B (now initialized with zeroes) is to later enable a smooth transition
		// to a more fine-grained partitioning scheme where
		// large organisations can go to several partitions instead of just one.
		key := make([]byte, 8)
		binary.LittleEndian.PutUint32(key, uint32(metric.OrgId))
		payload[i] = &sarama.ProducerMessage{
			Key:   sarama.ByteEncoder(key),
			Topic: topic,
			Value: sarama.ByteEncoder(data),
		}
		messagesSize.Value(int64(len(data)))
	}
	err := producer.SendMessages(payload)
	if err != nil {
		if errors, ok := err.(sarama.ProducerErrors); ok {
			for i := 0; i < 10 && i < len(errors); i++ {
				log.Error(4, "ProducerError %d/%d: %s", i, len(errors), errors[i].Error())
			}
		}
		return err
	}
	publishDuration.Value(time.Since(pre))
	metricsPublished.Inc(int64(len(metrics)))
	messagesPublished.Inc(int64(len(metrics)))
	metricsPerMessage.Value(1)

	log.Info("published %d metrics", len(data))
	return nil
}
