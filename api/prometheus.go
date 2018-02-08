package api

import (
	"io/ioutil"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/grafana/metrictank/conf"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/raintank/tsdb-gw/metric_publish"
	"github.com/raintank/tsdb-gw/metricpool"
	"github.com/raintank/worldping-api/pkg/log"
	schema "gopkg.in/raintank/schema.v1"
)

var (
	metricPool = metricpool.NewMetricDataPool()
)

func getSchemas(file string) (*conf.Schemas, error) {
	schemas, err := conf.ReadSchemas(file)
	if err != nil {
		return nil, err
	}
	return &schemas, nil
}

func PrometheusWrite(ctx *Context) {
	defer ctx.Req.Request.Body.Close()
	if ctx.Req.Request.Body != nil {
		compressed, err := ioutil.ReadAll(ctx.Req.Request.Body)

		if err != nil {
			ctx.JSON(400, err.Error())
			log.Error(3, "Read Error, %v", err)
			return
		}
		reqBuf, err := snappy.Decode(nil, compressed)
		if err != nil {
			ctx.JSON(400, err.Error())
			log.Error(3, "Decode Error, %v", err)
			return
		}

		var req prompb.WriteRequest
		if err := proto.Unmarshal(reqBuf, &req); err != nil {
			ctx.JSON(400, err.Error())
			log.Error(3, "Unmarshal Error, %v", err)
			return
		}

		buf := make([]*schema.MetricData, 0)

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
					*md = schema.MetricData{
						Name:   name,
						Metric: name,
						Value:  sample.Value,
						Unit:   "unknown",
						Time:   (sample.Timestamp / 1000),
						Mtype:  "gauge",
						Tags:   tagSet,
						OrgId:  ctx.OrgId,
					}
					md.SetId()
					buf = append(buf, md)
				}
			} else {
				log.Warn("prometheus metric received with empty name")
			}
		}

		err = metric_publish.Publish(buf)
		if err != nil {
			log.Error(3, "failed to publish prom write metrics. %s", err)
			ctx.JSON(500, err)
			return
		}
		ctx.JSON(200, "ok")
		return
	}
	ctx.JSON(400, "no data included in request.")
}
