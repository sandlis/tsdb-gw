package event_publish

import (
	"encoding/binary"
	"time"

	"github.com/Shopify/sarama"
	"github.com/raintank/met"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
	"gopkg.in/raintank/schema.v1/msg"
)

var (
	config          *sarama.Config
	producer        sarama.SyncProducer
	topic           string
	brokers         []string
	eventsPublished met.Count
	messagesSize    met.Meter
	publishDuration met.Timer
	sendErrProducer met.Count
	sendErrOther    met.Count
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
	config.Producer.Return.Successes = true
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
	eventsPublished = metrics.NewCount("eventpublisher.events-published")
	messagesSize = metrics.NewMeter("eventpublisher.message_size", 0)
	publishDuration = metrics.NewTimer("eventpublisher.publish_duration", 0)
	sendErrProducer = metrics.NewCount("eventpublisher.errors.producer")
	sendErrOther = metrics.NewCount("eventpublisher.errors.other")
}

func Publish(event *schema.ProbeEvent) error {
	if producer == nil {
		log.Debug("dropping event as publishing is disabled")
		return nil
	}

	id := time.Now().UnixNano()
	data, err := msg.CreateProbeEventMsg(event, id, msg.FormatProbeEventMsgp)
	if err != nil {
		log.Fatal(4, "Fatal error creating event message: %s", err)
	}
	payload := make([]*sarama.ProducerMessage, 1)
	// partition by organisation: metrics for the same org should go to the same
	// partition/MetricTank (optimize for locality~performance)
	// the extra 4B (now initialized with zeroes) is to later enable a smooth transition
	// to a more fine-grained partitioning scheme where
	// large organisations can go to several partitions instead of just one.
	key := make([]byte, 8)
	binary.LittleEndian.PutUint32(key, uint32(event.OrgId))
	payload[0] = &sarama.ProducerMessage{
		Key:   sarama.ByteEncoder(key),
		Topic: topic,
		Value: sarama.ByteEncoder(data),
	}
	messagesSize.Value(int64(len(data)))

	pre := time.Now()
	err = producer.SendMessages(payload)
	if err != nil {
		if errors, ok := err.(sarama.ProducerErrors); ok {
			sendErrProducer.Inc(int64(len(errors)))
			for i := 0; i < 10 && i < len(errors); i++ {
				log.Error(4, "SendMessages ProducerError %d/%d: %s", i, len(errors), errors[i].Error())
			}
		} else {
			sendErrOther.Inc(1)
			log.Error(4, "SendMessages error: %s", err.Error())
		}
		return err
	}
	publishDuration.Value(time.Since(pre))
	eventsPublished.Inc(int64(len(payload)))

	log.Info("published %d events", len(payload))
	return nil
}
