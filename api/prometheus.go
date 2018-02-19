package api

import (
	"flag"
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
	schemas    *conf.Schemas
	schemaFile = flag.String("prom-schemas-file", "/etc/storage-schemas.conf", "path to carbon storage-schemas.conf file for prom metrics")
	enabled    = flag.Bool("prometheus-enabled", false, "enable prometheus input")
)

func PrometheusInit() {
	if !*enabled {
		return
	}
	log.Info("prometheus input enabled")
	s, err := conf.ReadSchemas(*schemaFile)
	if err != nil {
		log.Fatal(4, "failed to load prometheus schemas config. %s", err)
	}
	schemas = &s
}

func PrometheusWrite(ctx *Context) {
	if ctx.Req.Request.Body != nil {
		defer ctx.Req.Request.Body.Close()
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

			_, s := schemas.Match(name, 0)

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
						Name:     name,
						Metric:   name,
						Interval: s.Retentions[0].SecondsPerPoint,
						Value:    sample.Value,
						Unit:     "unknown",
						Time:     (sample.Timestamp / 1000),
						Mtype:    "gauge",
						Tags:     tagSet,
						OrgId:    ctx.OrgId,
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
