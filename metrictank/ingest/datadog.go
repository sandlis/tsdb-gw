package ingest

import (
	"compress/zlib"
	"encoding/json"
	"io/ioutil"
	"sort"
	"strings"

	"github.com/DataDog/datadog-agent/pkg/metrics"
	"github.com/raintank/tsdb-gw/api"
	"github.com/raintank/tsdb-gw/publish"
	log "github.com/sirupsen/logrus"
	schema "gopkg.in/raintank/schema.v1"
)

func DataDogMTWrite(ctx *api.Context) {
	if ctx.Req.Request.Body != nil {
		defer ctx.Req.Request.Body.Close()

		var body []byte
		var err error
		if ctx.Req.Request.Header.Get("Content-Encoding") == "deflate" {
			zr, err := zlib.NewReader(ctx.Req.Request.Body)
			if err != nil {
				panic(err)
			}
			body, err = ioutil.ReadAll(zr)
		} else {
			body, err = ioutil.ReadAll(ctx.Req.Request.Body)
		}

		var series DataDogData
		err = json.Unmarshal(body, &series)
		if err != nil {
			return
		}

		buf := make([]*schema.MetricData, 0)
		for _, ts := range series.Series {
			_, s := schemas.Match(ts.Name, 0)
			tagSet := createTagSet(ts)
			for _, point := range ts.Points {
				md := metricPool.Get()
				*md = schema.MetricData{
					Name:     ts.Name,
					Metric:   ts.Name,
					Interval: s.Retentions[0].SecondsPerPoint,
					Value:    point.Value,
					Unit:     "unknown",
					Time:     int64(point.Ts),
					Mtype:    "gauge",
					Tags:     tagSet,
					OrgId:    ctx.ID,
				}
				md.SetId()
				buf = append(buf, md)
			}
		}

		err = publish.Publish(buf)
		for _, m := range buf {
			metricPool.Put(m)
		}
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

type DataDogData struct {
	Series metrics.Series `json:"series,omitempty"`
}

func createTagSet(serie *metrics.Serie) []string {
	tags := []string{}
	if serie.Device != "" {
		tags = append(tags, "device="+serie.Device)
	}
	tags = append(tags, "host="+serie.Host)
	for _, t := range serie.Tags {
		tSplit := strings.SplitN(":", t, 2)
		if len(tSplit) == 0 {
			continue
		}
		if len(tSplit) == 1 {
			tags = append(tags, tSplit[0])
			continue
		}
		if tSplit[1] == "" {
			tags = append(tags, tSplit[0])
			continue
		}
		tags = append(tags, tSplit[0]+"="+tSplit[1])
	}
	sort.Strings(tags)
	return tags
}
