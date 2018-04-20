package kafka

import (
	"errors"
	"flag"
	"fmt"
	"hash"
	"hash/fnv"
	"sync"
	"time"

	"github.com/grafana/metrictank/conf"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	p "github.com/grafana/metrictank/cluster/partitioner"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/tsdb-gw/publish/kafka/keycache"
	"github.com/raintank/tsdb-gw/usage"
	"github.com/raintank/tsdb-gw/util"
	log "github.com/sirupsen/logrus"
	"gopkg.in/raintank/schema.v1"
	"gopkg.in/raintank/schema.v1/msg"
)

var (
	config   *kafka.ConfigMap
	producer *kafka.Producer
	brokers  []string
	keyCache *keycache.KeyCache
	// partitioner only needs to be initialized once since its configuration
	// won't change during runtime and a single instance can be used by many
	// threads
	kafkaPartitioner *p.Kafka

	schemasConf string

	publishedMD     = stats.NewCounter32("output.kafka.published.metricdata")
	publishedMP     = stats.NewCounter32("output.kafka.published.metricpoint")
	publishedMPNO   = stats.NewCounter32("output.kafka.published.metricpoint_no_org")
	messagesSize    = stats.NewMeter32("metrics.message_size", false)
	publishDuration = stats.NewLatencyHistogram15s32("metrics.publish")
	sendErrProducer = stats.NewCounter32("metrics.send_error.producer")
	sendErrOther    = stats.NewCounter32("metrics.send_error.other")

	topic            string
	codec            string
	enabled          bool
	partitionScheme  string
	maxInFlight      int
	bufferMaxMs      int
	bufferMaxMsgs    int
	batchNumMessages int
	partitionCount   int32
	v2               bool
	v2Org            bool
	v2StaleThresh    time.Duration
	v2PruneInterval  time.Duration

	bufferPool      = util.NewBufferPool()
	bufferPool33    = util.NewBufferPool33()
	partitionerPool sync.Pool
)

type mtPublisher struct {
	schemas *conf.Schemas
}

type Partitioner interface {
	partition(schema.PartitionedMetric) (int32, []byte, error)
}

func NewPartitioner() Partitioner {
	return &partitionerFnv1a{
		hasher: fnv.New32a(),
	}
}

type partitionerFnv1a struct {
	hasher hash.Hash32
}

func (p *partitionerFnv1a) partition(m schema.PartitionedMetric) (int32, []byte, error) {
	key, err := kafkaPartitioner.GetPartitionKey(m, nil)
	if err != nil {
		return -1, nil, err
	}

	p.hasher.Reset()
	_, err = p.hasher.Write(key)
	if err != nil {
		return -1, nil, err
	}

	partition := int32(p.hasher.Sum32()) % partitionCount
	if partition < 0 {
		partition = -partition
	}

	return partition, key, nil
}

func init() {
	flag.StringVar(&topic, "metrics-topic", "mdm", "topic for metrics")
	flag.StringVar(&codec, "metrics-kafka-comp", "snappy", "compression: none|gzip|snappy|lz4")
	flag.BoolVar(&enabled, "metrics-publish", false, "enable metric publishing")
	flag.StringVar(&partitionScheme, "metrics-partition-scheme", "bySeries", "method used for paritioning metrics. (byOrg|bySeries)")
	flag.IntVar(&maxInFlight, "metrics-max-in-flight", 1000000, "The maximum number of requests in flight per broker connection")
	flag.IntVar(&bufferMaxMsgs, "metrics-buffer-max-msgs", 100000, "Maximum number of messages allowed on the producer queue. Publishing attempts will be rejected once this limit is reached.")
	flag.IntVar(&bufferMaxMs, "metrics-buffer-max-ms", 100, "Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers")
	flag.IntVar(&batchNumMessages, "batch-num-messages", 10000, "Maximum number of messages batched in one MessageSet")

	flag.BoolVar(&v2, "v2", true, "enable optimized MetricPoint payload")
	flag.BoolVar(&v2Org, "v2-org", true, "encode org-id in messages")
	flag.DurationVar(&v2StaleThresh, "v2-stale-thresh", 6*time.Hour, "expire keys (and resend MetricData if seen again) if not seen for this much time")
	flag.DurationVar(&v2PruneInterval, "v2-prune-interval", time.Hour, "check interval for expiring keys")

	flag.StringVar(&schemasConf, "schemas-file", "/etc/gw/storage-schemas.conf", "path to carbon storage-schemas.conf file")
}

func New(broker string) *mtPublisher {
	if !enabled {
		return nil
	}
	var err error

	schemas, err := getSchemas(schemasConf)
	if err != nil {
		log.Fatalf("failed to load schemas config. %s", err)
	}

	if codec != "none" && codec != "gzip" && codec != "snappy" && codec != "lz4" {
		log.Fatalf("invalid compression codec. must be one of: none|gzip|snappy|lz4")
	}

	config := kafka.ConfigMap{}
	config.SetKey("request.required.acks", "all")
	config.SetKey("message.send.max.retries", "10")
	config.SetKey("bootstrap.servers", broker)
	config.SetKey("compression.codec", codec)
	config.SetKey("max.in.flight", maxInFlight)
	config.SetKey("queue.buffering.max.ms", bufferMaxMs)
	config.SetKey("batch.num.messages", batchNumMessages)
	config.SetKey("queue.buffering.max.messages", bufferMaxMsgs)

	producer, err = kafka.NewProducer(&config)
	if err != nil {
		log.Fatalf("failed to initialize kafka producer. %s", err)
	}

	meta, err := tryGetMetadata(producer, topic, 3)
	if err != nil {
		log.Fatalf("Failed to get topics from kafka: %s", err)
	}

	partitionCount = int32(len(meta.Topics[topic].Partitions))
	kafkaPartitioner, err = p.NewKafka(partitionScheme)
	if err != nil {
		log.Fatalf("failed to initialize partitioner. %s", err)
	}

	if v2 {
		keyCache = keycache.NewKeyCache(v2StaleThresh, v2PruneInterval)
	}

	partitionerPool = sync.Pool{
		New: func() interface{} { return NewPartitioner() },
	}
	return &mtPublisher{
		schemas: schemas,
	}
}

func tryGetMetadata(producer *kafka.Producer, topic string, attempts int) (*kafka.Metadata, error) {
	for attempt := 0; attempt < attempts; attempt++ {
		meta, err := producer.GetMetadata(&topic, false, 30000)
		if err != nil {
			return nil, err
		}

		if t, ok := meta.Topics[topic]; ok && len(t.Partitions) > 0 {
			return meta, nil
		}
		time.Sleep(time.Second)
	}

	return nil, fmt.Errorf("Could not get partitions after %d attempts", attempts)
}

func (m *mtPublisher) Publish(metrics []*schema.MetricData) error {
	if producer == nil {
		log.Debugf("dropping %d metrics as publishing is disabled", len(metrics))
		return nil
	}
	if len(metrics) == 0 {
		return nil
	}
	var err error

	payload := make([]*kafka.Message, len(metrics))
	pre := time.Now()
	deliveryChan := make(chan kafka.Event, len(metrics))
	partitioner := partitionerPool.Get().(Partitioner)

	pubMD := 0
	pubMP := 0
	pubMPNO := 0

	for i, metric := range metrics {
		if metric.Interval == 0 {
			_, s := m.schemas.Match(metric.Name, 0)
			metric.Interval = s.Retentions[0].SecondsPerPoint
			metric.SetId()
		}

		var data []byte
		if v2 {
			var mkey schema.MKey
			mkey, err = schema.MKeyFromString(metric.Id)
			if err != nil {
				return err
			}
			ok := keyCache.Touch(mkey, pre)
			// we've seen this key recently. we can use the optimized format
			if ok {
				data = bufferPool33.Get()
				mp := schema.MetricPoint{
					MKey:  mkey,
					Value: metric.Value,
					Time:  uint32(metric.Time),
				}
				if v2Org {
					data = data[:33]                      // this range will contain valid data
					data[0] = byte(msg.FormatMetricPoint) // store version in first byte
					_, err = mp.Marshal32(data[:1])       // Marshal will fill up space between length and cap, i.e. bytes 2-33
					pubMP++
				} else {
					data = data[:29]                                // this range will contain valid data
					data[0] = byte(msg.FormatMetricPointWithoutOrg) // store version in first byte
					_, err = mp.MarshalWithoutOrg28(data[:1])       // Marshal will fill up space between length and cap, i.e. bytes 2-29
					pubMPNO++
				}
			} else {
				data = bufferPool.Get()
				data, err = metric.MarshalMsg(data)
				if err != nil {
					return err
				}
				pubMD++
			}
		} else {
			data = bufferPool.Get()
			data, err = metric.MarshalMsg(data)
			if err != nil {
				return err
			}
			pubMD++
		}

		part, key, err := partitioner.partition(metric)
		if err != nil {
			return err
		}

		payload[i] = &kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: part},
			Value:          data,
			Key:            key,
		}

		messagesSize.Value(len(data))

		err = producer.Produce(payload[i], deliveryChan)
		if err != nil {
			return err
		}
	}

	partitionerPool.Put(partitioner)

	// return buffers to the bufferPool
	defer func() {
		var buf []byte
		for _, msg := range payload {
			buf = msg.Value
			if cap(buf) == 33 {
				bufferPool33.Put(buf)
			} else {
				bufferPool.Put(buf)
			}
		}
	}()

	msgCount := 0
	var errCount int
	var firstErr error
	for e := range deliveryChan {
		msgCount++

		err = nil
		m, ok := e.(*kafka.Message)
		if !ok || e == nil {
			log.Errorf("unexpected delivery report of type %T: %v", e, e)
			err = errors.New("Invalid acknowledgement")
		} else if m.TopicPartition.Error != nil {
			err = m.TopicPartition.Error
		}

		if err != nil {
			errCount++
			sendErrOther.Inc()
			if firstErr == nil {
				firstErr = err
			}
		}

		if msgCount >= len(metrics) {
			close(deliveryChan)
		}
	}

	if firstErr != nil {
		log.Errorf("Got %d errors when sending %d messages, the first was: %s", errCount, len(metrics), firstErr)
		return firstErr
	}

	publishDuration.Value(time.Since(pre))
	publishedMD.Add(pubMD)
	publishedMP.Add(pubMP)
	publishedMPNO.Add(pubMPNO)
	log.Debugf("published %d metrics", pubMD+pubMP)
	for _, metric := range metrics {
		usage.LogDataPoint(metric.Id)
	}
	return nil
}

func (*mtPublisher) Type() string {
	return "Metrictank"
}
