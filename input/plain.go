package input

import (
	"bufio"
	"io"
	"net"

	m20 "github.com/metrics20/go-metrics20/carbon20"
	"github.com/raintank/tsdb-gw/metric_publish"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

type Plain struct {
}

func NewPlain(addr string) (net.Listener, error) {
	l, err := listen(addr, &Plain{})
	if err != nil {
		return nil, err
	}
	return l, nil
}

func (p *Plain) Handle(c net.Conn) {
	defer c.Close()
	// TODO c.SetTimeout(60e9)
	r := bufio.NewReaderSize(c, 4096)
	for {

		// Note that everything in this loop should proceed as fast as it can
		// so we're not blocked and can keep processing
		// so the validation, the pipeline initiated via metrics_public.Publish(), etc
		// must never block.

		// note that we don't support lines longer than 4096B. that seems very reasonable..
		buf, _, err := r.ReadLine()

		if err != nil {
			if io.EOF != err {
				log.Error(3, err.Error())
			}
			break
		}

		// numIn.Inc(1)

		name, val, ts, err := m20.ValidatePacket(buf, m20.MediumLegacy, m20.MediumM20)
		if err != nil {
			// numInvalid.Inc(1)
			continue
		}

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

		metrics := make([]*schema.MetricData, 0)
		metrics = append(metrics, &md)

		err = metric_publish.Publish(metrics)
		if err != nil {
			log.Error(3, "failed to publish metrics. %s", err)
		}
	}
}
