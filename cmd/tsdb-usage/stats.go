package main

import (
	"flag"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/golang/glog"
	"github.com/raintank/tsdb-gw/publish"
	"github.com/raintank/tsdb-gw/usage"
	"gopkg.in/raintank/schema.v1"
)

var shardCount = 4
var bufferSize = 100
var metricMaxStale = time.Hour * time.Duration(6)
var flushInterval = time.Minute

var eventsChan chan *usage.Event
var shards []chan *usage.Event

func init() {
	flag.IntVar(&shardCount, "shard-count", 4, "Number of shards. Should equal number of CPU cores")
	flag.IntVar(&bufferSize, "buffer-size", 100, "per shard input buffer size")
	flag.DurationVar(&metricMaxStale, "metric-max-stale", metricMaxStale, "if a metric has not been seen for this duration, it is not considered active")
	flag.DurationVar(&flushInterval, "flush-interval", time.Minute, "interval at which to emit usage stats.")
}

func Init() {
	eventsChan = make(chan *usage.Event, bufferSize)
	shards = make([]chan *usage.Event, shardCount)
	for i := 0; i < shardCount; i++ {
		shards[i] = make(chan *usage.Event, bufferSize)
	}
}

// per shard data structure for keeping track of
// org usage between flushes
type ShardStats struct {
	Orgs map[int32]*OrgStats
}

type OrgStats struct {
	ActiveSeries  map[string]uint32
	RequestCounts map[string]int64
	DataPoints    int64
}

// per org stats emitted at every flush cycle
type TsdbStats struct {
	Orgs map[int32]*TsdbOrgStats
}

type TsdbOrgStats struct {
	ActiveSeries  int64
	RequestCounts map[string]int64
	Rate          float64
}

func Record(e *usage.Event) {
	eventsChan <- e
}

func StatsRun() {
	flushChan := make(chan TsdbStats, shardCount)
	go emitStats(flushChan)
	for i := 0; i < shardCount; i++ {
		go routeToShard()
		go shardIndexer(i, flushChan)
	}
}

func emitStats(flushChan chan TsdbStats) {
	ticker := time.NewTicker(flushInterval)
	for ts := range ticker.C {
		totals := TsdbStats{
			Orgs: make(map[int32]*TsdbOrgStats),
		}
		for i := 0; i < shardCount; i++ {
			summary := <-flushChan
			for org, stat := range summary.Orgs {
				if _, ok := totals.Orgs[org]; !ok {
					totals.Orgs[org] = &TsdbOrgStats{
						RequestCounts: make(map[string]int64),
					}
				}
				totals.Orgs[org].ActiveSeries += stat.ActiveSeries
				totals.Orgs[org].Rate += stat.Rate
				for path, count := range stat.RequestCounts {
					totals.Orgs[org].RequestCounts[path] += count
				}
			}
		}
		metrics := make([]*schema.MetricData, 0)
		for org, s := range totals.Orgs {
			glog.V(4).Infof("org %d has %d active series and rate of %f", org, s.ActiveSeries, s.Rate)
			if org <= 0 {
				// dont emit metrics for the "public" org (-1) or metrics that have no org (0)
				continue
			}
			metrics = append(metrics, &schema.MetricData{
				Metric:   "hosted-metrics.usage.active_series",
				Name:     "hosted-metrics.usage.active_series",
				Interval: int(flushInterval.Seconds()),
				Mtype:    "gauge",
				OrgId:    int(org),
				Value:    float64(s.ActiveSeries),
				Time:     ts.Unix(),
			}, &schema.MetricData{
				Metric:   "hosted-metrics.usage.datapoints_per_minute",
				Name:     "hosted-metrics.usage.datapoints_per_minute",
				Interval: int(flushInterval.Seconds()),
				Mtype:    "rate",
				OrgId:    int(org),
				Value:    s.Rate * 60.0,
				Time:     ts.Unix(),
			})
			for path, count := range s.RequestCounts {
				metrics = append(metrics, &schema.MetricData{
					Metric:   fmt.Sprintf("hosted-metrics.usage.%s", path),
					Name:     fmt.Sprintf("hosted-metrics.usage.%s", path),
					Interval: int(flushInterval.Seconds()),
					Mtype:    "rate",
					OrgId:    int(org),
					Value:    float64(count),
					Time:     ts.Unix(),
				})
			}
		}
		for _, m := range metrics {
			m.SetId()
		}
		if err := publish.Publish(metrics); err != nil {
			glog.Errorf("failed to publish metrics. %v", err)
		}

	}
}

func routeToShard() {
	var e *usage.Event
	var shard int
	hasher := fnv.New32a()
	for e = range eventsChan {
		hasher.Reset()
		hasher.Write([]byte(e.ID))
		shard = int(hasher.Sum32() % uint32(shardCount))
		glog.V(6).Infof("routing %d - %s to shard %d", e.Org, e.ID, shard)
		shards[shard] <- e
	}
}

func shardIndexer(shard int, flushChan chan TsdbStats) {
	glog.Infof("Shard %d starting up", shard)
	idx := ShardStats{
		Orgs: make(map[int32]*OrgStats),
	}
	cleanUpTicker := time.NewTicker(time.Hour)
	flushTicker := time.NewTicker(flushInterval)
	lastFlush := time.Now()
	var e *usage.Event
	var stats *OrgStats
	var ok bool
	for {
		select {
		case e = <-shards[shard]:
			if stats, ok = idx.Orgs[e.Org]; !ok {
				glog.Infof("new org, %d,  seen on shard %d", e.Org, shard)
				stats = &OrgStats{
					ActiveSeries:  make(map[string]uint32),
					RequestCounts: make(map[string]int64),
					DataPoints:    0,
				}
				idx.Orgs[e.Org] = stats
			}
			switch e.EType {
			case usage.DataPointReceived:
				stats.DataPoints++
				stats.ActiveSeries[e.ID] = uint32(time.Now().Unix())
			case usage.ApiRequest:
				stats.RequestCounts[e.ID]++
			}

		case <-cleanUpTicker.C:
			old := uint32(time.Now().Add(metricMaxStale * time.Duration(-1)).Unix())
			for org, stats := range idx.Orgs {
				for id, lastSeen := range stats.ActiveSeries {
					if lastSeen < old {
						delete(stats.ActiveSeries, id)
					}
				}
				if len(stats.ActiveSeries) == 0 {
					glog.Info("org %d has no active series.", org)
					delete(idx.Orgs, org)
				}
			}
		case <-flushTicker.C:
			summary := TsdbStats{
				Orgs: make(map[int32]*TsdbOrgStats),
			}
			for org, stats := range idx.Orgs {
				glog.V(5).Infof("org %d has %d activeSeries and metricCount %d on shard %d", org, len(stats.ActiveSeries), stats.DataPoints, shard)
				summary.Orgs[org] = &TsdbOrgStats{
					ActiveSeries:  int64(len(stats.ActiveSeries)),
					Rate:          float64(stats.DataPoints) / time.Since(lastFlush).Seconds(),
					RequestCounts: make(map[string]int64),
				}
				stats.DataPoints = 0
				for path, count := range stats.RequestCounts {
					summary.Orgs[org].RequestCounts[path] = count
					stats.RequestCounts[path] = 0
				}
			}
			lastFlush = time.Now()
			flushChan <- summary
		}
	}

}
