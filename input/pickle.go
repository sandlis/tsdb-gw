package input

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math/big"
	"net"

	ogorek "github.com/kisielk/og-rek"
	m20 "github.com/metrics20/go-metrics20/carbon20"
	"github.com/raintank/tsdb-gw/metric_publish"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

type Pickle struct {
}

func NewPickle(addr string) (net.Listener, error) {
	l, err := listen(addr, &Pickle{})
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (p *Pickle) Handle(c net.Conn) {
	defer c.Close()
	// TODO c.SetTimeout(60e9)
	r := bufio.NewReaderSize(c, 4096)
	log.Debug("pickle.go: entering ReadLoop...")
ReadLoop:
	for {

		// Note that everything in this loop should proceed as fast as it can
		// so we're not blocked and can keep processing
		// so the validation, the pipeline initiated via metrics_public.Publish(), etc
		// must never block.

		log.Debug("pickle.go: detecting payload length with binary.Read...")
		var length uint32
		err := binary.Read(r, binary.BigEndian, &length)
		if err != nil {
			if io.EOF != err {
				log.Error(3, "couldn't read payload length: "+err.Error())
			}
			log.Debug("pickle.go: detected EOF while detecting payload length with binary.Read, nothing more to read, breaking")
			break
		}
		log.Debug(fmt.Sprintf("pickle.go: done detecting payload length with binary.Read, length is %d", int(length)))

		lengthTotal := int(length)
		lengthRead := 0
		payload := make([]byte, lengthTotal, lengthTotal)
		for {
			log.Debug("pickle.go: reading payload...")
			tmpLengthRead, err := r.Read(payload[lengthRead:])
			if err != nil {
				log.Error(3, "couldn't read payload: "+err.Error())
				break ReadLoop
			}
			lengthRead += tmpLengthRead
			if lengthRead == lengthTotal {
				log.Debug("pickle.go: done reading payload")
				break
			}
			if lengthRead > lengthTotal {
				log.Error(3, fmt.Sprintf("expected to read %d bytes, but read %d", length, lengthRead))
				break ReadLoop
			}
		}

		decoder := ogorek.NewDecoder(bytes.NewBuffer(payload))

		log.Debug("pickle.go: decoding pickled data...")
		rawDecoded, err := decoder.Decode()
		if err != nil {
			if io.ErrUnexpectedEOF != err {
				log.Error(3, "error reading pickled data "+err.Error())
			}
			log.Debug("pickle.go: detected ErrUnexpectedEOF while decoding pickled data, nothing more to decode, breaking")
			break
		}
		log.Debug("pickle.go: done decoding pickled data")

		log.Debug("pickle.go: checking the type of pickled data...")
		decoded, ok := rawDecoded.([]interface{})
		if !ok {
			log.Error(3, fmt.Sprintf("Unrecognized type %T for pickled data", rawDecoded))
			break
		}
		log.Debug("pickle.go: done checking the type of pickled data")

		log.Debug("pickle.go: entering ItemLoop...")

		metrics := make([]*schema.MetricData, 0)

	ItemLoop:
		for _, rawItem := range decoded {
			log.Debug("pickle.go: doing high-level validation of unpickled item and data...")
			item, ok := rawItem.(ogorek.Tuple)
			if !ok {
				log.Error(3, fmt.Sprintf("Unrecognized type %T for item", rawItem))
				continue
			}
			if len(item) != 2 {
				log.Error(3, fmt.Sprintf("item length must be 2, got %d", len(item)))
				continue
			}

			metric, ok := item[0].(string)
			if !ok {
				log.Error(3, fmt.Sprintf("item metric must be a string, got %T", item[0]))
				continue
			}

			data, ok := item[1].(ogorek.Tuple)
			if !ok {
				log.Error(3, fmt.Sprintf("item data must be an array, got %T", item[1]))
				continue
			}
			if len(data) != 2 {
				log.Error(3, fmt.Sprintf("item data length must be 2, got %d", len(data)))
				continue
			}
			log.Debug("pickle.go: done doing high-level validation of unpickled item and data")

			var value string
			switch data[1].(type) {
			case string:
				value = data[1].(string)
			case uint8, uint16, uint32, uint64, int8, int16, int32, int64:
				value = fmt.Sprintf("%d", data[1])
			case float32, float64:
				value = fmt.Sprintf("%f", data[1])
			default:
				log.Error(3, fmt.Sprintf("Unrecognized type %T for value", data[1]))
				continue ItemLoop
			}

			var timestamp string
			switch data[0].(type) {
			case string:
				timestamp = data[0].(string)
			case uint8, uint16, uint32, uint64, int8, int16, int32, int64, (*big.Int):
				timestamp = fmt.Sprintf("%d", data[0])
			case float32, float64:
				timestamp = fmt.Sprintf("%.0f", data[0])
			default:
				log.Error(3, fmt.Sprintf("Unrecognized type %T for timestamp", data[0]))
				continue ItemLoop
			}

			buf := []byte(metric + " " + value + " " + timestamp)

			log.Debug("pickle.go: passing unpickled metric to m20 Packet validator...")
			name, val, ts, err := m20.ValidatePacket(buf, m20.MediumLegacy, m20.MediumM20)
			if err != nil {
				log.Debug("pickle.go: metric failed to pass m20 Packet validation!")
				continue
			}

			log.Debug("pickle.go: all good, dispatching metrics buffer")

			// TODO
			interval := 60
			orgId := 1

			md := schema.MetricData{
				Name:     string(name),
				Metric:   string(name),
				Interval: interval,
				Value:    val,
				Unit:     "unknown",
				Time:     int64(ts),
				Mtype:    "gauge",
				Tags:     []string{},
				OrgId:    orgId,
			}
			md.SetId()

			metrics = append(metrics, &md)

			log.Debug("pickle.go: exiting ItemLoop")
		}

		err = metric_publish.Publish(metrics)
		if err != nil {
			log.Error(3, "failed to publish metrics. %s", err)
		}

		log.Debug("pickle.go: exiting ReadLoop")
	}
}
