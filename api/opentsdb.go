package api

import (
	"compress/gzip"
	"encoding/json"
	"io"
	"io/ioutil"

	"github.com/raintank/tsdb-gw/metric_publish"
	"github.com/raintank/tsdb-gw/opentsdb"
	"github.com/raintank/worldping-api/pkg/log"
	schema "gopkg.in/raintank/schema.v1"
)

func OpenTSDBWrite(ctx *Context) {
	if ctx.Req.Request.Body != nil {
		defer ctx.Req.Request.Body.Close()
		var reader io.Reader
		var err error
		if ctx.Req.Header.Get("Content-Encoding") == "gzip" {
			reader, err = gzip.NewReader(ctx.Req.Request.Body)
			if err != nil {
				ctx.JSON(400, err.Error())
				log.Error(3, "Read Error, %v", err)
				return
			}
		} else {
			reader = ctx.Req.Request.Body
		}

		body, err := ioutil.ReadAll(reader)
		if err != nil {
			ctx.JSON(400, err.Error())
			log.Error(3, "Read Error, %v", err)
			return
		}

		var req opentsdb.PutRequest
		err = json.Unmarshal(body, &req)
		if err != nil {
			ctx.JSON(400, err.Error())
			log.Error(3, "Read Error, %v", err)
			return
		}
		buf := make([]*schema.MetricData, 0)
		for _, ts := range req {
			_, s := schemas.Match(ts.Metric, 0)
			md := metricPool.Get()
			*md = schema.MetricData{
				Name:     ts.Metric,
				Metric:   ts.Metric,
				Interval: s.Retentions[0].SecondsPerPoint,
				Value:    ts.Value,
				Unit:     "unknown",
				Time:     ts.Timestamp,
				Mtype:    "gauge",
				Tags:     ts.FormatTags(),
				OrgId:    ctx.OrgId,
			}
			md.SetId()
			buf = append(buf, md)

		}

		err = metric_publish.Publish(buf)
		for _, m := range buf {
			metricPool.Put(m)
		}
		if err != nil {
			log.Error(3, "failed to publish opentsdb write metrics. %s", err)
			ctx.JSON(500, err)
			return
		}
		ctx.JSON(200, "ok")
		return
	}

	ctx.JSON(400, "no data included in request.")
}
