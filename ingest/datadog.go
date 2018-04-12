package ingest

import (
	"compress/zlib"
	"encoding/json"
	"io/ioutil"
	"sort"
	"strings"

	"github.com/raintank/tsdb-gw/api"
	"github.com/raintank/tsdb-gw/publish"
	log "github.com/sirupsen/logrus"
	schema "gopkg.in/raintank/schema.v1"
)

// DataDogPayload struct to unmarshal datadog agent json
type DataDogPayload struct {
	Series []struct {
		Name   string       `json:"metric"`
		Points [][2]float64 `json:"points"`
		Tags   []string     `json:"tags"`
		Host   string       `json:"host"`
		Mtype  string       `json:"type"`
		Device string       `json:"device,omitempty"`
	} `json:"series"`
}

func DataDogWrite(ctx *api.Context) {
	if ctx.Req.Request.Body == nil {
		ctx.JSON(400, "no data included in request.")
		return
	}
	defer ctx.Req.Request.Body.Close()

	var body []byte
	var err error
	if ctx.Req.Request.Header.Get("Content-Encoding") == "deflate" {
		zr, err := zlib.NewReader(ctx.Req.Request.Body)
		if err != nil {
			log.Errorf("unable to decode json: %v", err)
			ctx.JSON(500, err)
			return
		}
		body, err = ioutil.ReadAll(zr)
	} else {
		body, err = ioutil.ReadAll(ctx.Req.Request.Body)
	}

	var series DataDogPayload
	err = json.Unmarshal(body, &series)
	if err != nil {
		return
	}

	buf := make([]*schema.MetricData, 0)
	for _, ts := range series.Series {
		_, s := schemas.Match(ts.Name, 0)
		tagSet := createTagSet(ts.Host, ts.Device, ts.Tags)
		for _, point := range ts.Points {
			md := metricPool.Get()
			*md = schema.MetricData{
				Name:     ts.Name,
				Interval: s.Retentions[0].SecondsPerPoint,
				Value:    point[1],
				Unit:     "unknown",
				Time:     int64(point[0]),
				Mtype:    ts.Mtype,
				Tags:     tagSet,
				OrgId:    ctx.ID,
			}
			md.SetId()
			buf = append(buf, md)
		}
	}

	err = publish.Publish(buf)

	defer func(buf []*schema.MetricData) {
		for _, m := range buf {
			metricPool.Put(m)
		}
	}(buf)

	if err != nil {
		log.Errorf("failed to publish datadog metrics. %s", err)
		ctx.JSON(500, err)
		return
	}
	ctx.JSON(200, "ok")
	return
}

func createTagSet(host string, device string, ctags []string) []string {
	tags := []string{}
	if device != "" {
		tags = append(tags, "device="+device)
	}
	tags = append(tags, "host="+host)
	for _, t := range ctags {
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
