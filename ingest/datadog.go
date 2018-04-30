package ingest

import (
	"compress/zlib"
	"encoding/json"
	"io/ioutil"
	"sort"
	"strings"

	"github.com/raintank/tsdb-gw/api"
	"github.com/raintank/tsdb-gw/persister/persist"
	"github.com/raintank/tsdb-gw/publish"
	log "github.com/sirupsen/logrus"
	schema "gopkg.in/raintank/schema.v1"
)

// DataDogSeriesPayload struct to unmarshal datadog agent json
type DataDogSeriesPayload struct {
	Series []struct {
		Name   string       `json:"metric"`
		Points [][2]float64 `json:"points"`
		Tags   []string     `json:"tags"`
		Host   string       `json:"host"`
		Mtype  string       `json:"type"`
		Device string       `json:"device,omitempty"`
	} `json:"series"`
}

func DataDogSeries(ctx *api.Context) {
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

	var series DataDogSeriesPayload
	err = json.Unmarshal(body, &series)
	if err != nil {
		return
	}

	buf := make([]*schema.MetricData, 0)
	for _, ts := range series.Series {
		tagSet := createTagSet(ts.Host, ts.Device, ts.Tags)
		for _, point := range ts.Points {
			md := metricPool.Get()
			*md = schema.MetricData{
				Name:     ts.Name,
				Interval: 0,
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

// DataDogSeriesPayload struct to unmarshal datadog agent json
type DataDogCheckPayload []struct {
	Check     string   `json:"check"`
	Host      string   `json:"host_name"`
	Timestamp int64    `json:"timestamp"`
	Status    float64  `json:"status"`
	Message   string   `json:"message"`
	Tags      []string `json:"tags"`
}

func DataDogCheck(ctx *api.Context) {
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

	var checks DataDogCheckPayload
	err = json.Unmarshal(body, &checks)
	if err != nil {
		return
	}

	buf := make([]*schema.MetricData, 0)
	for _, check := range checks {
		tagSet := createTagSet(check.Host, "", check.Tags)
		md := metricPool.Get()
		*md = schema.MetricData{
			Name:     check.Check,
			Interval: 0,
			Value:    check.Status,
			Unit:     "unknown",
			Time:     check.Timestamp,
			Mtype:    "gauge",
			Tags:     tagSet,
			OrgId:    ctx.ID,
		}
		md.SetId()
		buf = append(buf, md)
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
		tSplit := strings.SplitN(t, ":", 2)
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

type DataDogIntakePayload struct {
	AgentVersion string `json:"agentVersion"`
	OS           string `json:"os"`
	SystemStats  struct {
		Machine   string `json:"machine"`
		Processor string `json:"processor"`
	} `json:"systemStats"`
	Meta struct {
		SocketHostname string `json:"socket-hostname"`
		SocketFqdn     string `json:"socket-fqdn"`
		Hostname       string `json:"hostname"`
	} `json:"meta"`
	Gohai string `json:"gohai"`
}

func (i *DataDogIntakePayload) GeneratePersistantMetrics(orgID int) []*schema.MetricData {
	metrics := []*schema.MetricData{}
	systemTags := []string{
		"agentVersion=" + i.AgentVersion,
		"hostname=" + i.Meta.Hostname,
		"machine=" + i.SystemStats.Machine,
		"os=" + i.OS,
		"processor=" + i.SystemStats.Processor,
		"socket_fqdn=" + i.Meta.SocketFqdn,
		"socket_hostname=" + i.Meta.SocketHostname,
	}

	metrics = append(metrics, &schema.MetricData{
		Name:  "system_info",
		Tags:  systemTags,
		Value: 1,
		OrgId: orgID,
	})

	return metrics
}

func DataDogIntake(ctx *api.Context) {
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

	var info DataDogIntakePayload
	err = json.Unmarshal(body, &info)
	if err != nil {
		return
	}

	if info.Gohai != "" {
		err = persist.Persist(info.GeneratePersistantMetrics(ctx.ID))
		if err != nil {
			log.Errorf("failed to persist datadog info. %s", err)
			ctx.JSON(500, err)
			return
		}
	}

	ctx.JSON(200, "ok")
	return
}
