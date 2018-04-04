package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/tsdb-gw/api"
	"github.com/raintank/tsdb-gw/ingest"
	"github.com/raintank/tsdb-gw/ingest/carbon"
	"github.com/raintank/tsdb-gw/publish"
	"github.com/raintank/tsdb-gw/publish/kafka"
	"github.com/raintank/tsdb-gw/query/graphite"
	"github.com/raintank/tsdb-gw/query/metrictank"
	"github.com/raintank/tsdb-gw/usage"
	"github.com/raintank/tsdb-gw/util"
	log "github.com/sirupsen/logrus"
)

var (
	app         = "tsdb-gw"
	GitHash     = "(none)"
	showVersion = flag.Bool("version", false, "print version string")

	authPlugin = flag.String("api-auth-plugin", "grafana", "auth plugin to use. (grafana|file)")
	confFile   = flag.String("config", "/etc/gw.ini", "configuration file path")

	broker = flag.String("kafka-tcp-addr", "localhost:9092", "kafka tcp address for metrics")

	statsEnabled    = flag.Bool("stats-enabled", false, "enable sending graphite messages for instrumentation")
	statsPrefix     = flag.String("stats-prefix", "tsdb-gw.stats.default.$hostname", "stats prefix (will add trailing dot automatically if needed)")
	statsAddr       = flag.String("stats-addr", "localhost:2003", "graphite address")
	statsInterval   = flag.Int("stats-interval", 10, "interval in seconds to send statistics")
	statsBufferSize = flag.Int("stats-buffer-size", 20000, "how many messages (holding all measurements from one interval) to buffer up in case graphite endpoint is unavailable.")

	graphiteURL   = flag.String("graphite-url", "http://localhost:8080", "graphite-api address")
	metrictankURL = flag.String("metrictank-url", "http://localhost:6060", "metrictank address")

	tsdbStatsEnabled = flag.Bool("tsdb-stats-enabled", false, "enable collecting usage stats")
	tsdbStatsAddr    = flag.String("tsdb-stats-addr", "localhost:2004", "tsdb-usage server address")

	tracingEnabled = flag.Bool("tracing-enabled", false, "enable/disable distributed opentracing via jaeger")
	tracingAddr    = flag.String("tracing-addr", "localhost:6831", "address of the jaeger agent to send data to")

	metricsAddr = flag.String("metrics-addr", ":8001", "http service address for the /metrics endpoint")
)

func main() {
	flag.Parse()

	// Only try and parse the conf file if it exists
	path := ""
	if _, err := os.Stat(*confFile); err == nil {
		path = *confFile
	}
	conf, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename:  path,
		EnvPrefix: "GW_",
	})
	if err != nil {
		log.Fatalf("error with configuration file: %s", err)
		os.Exit(1)
	}
	conf.ParseAll()

	util.InitLogger()

	if *showVersion {
		fmt.Printf("tsdb (built with %s, git hash %s)\n", runtime.Version(), GitHash)
		return
	}

	if *statsEnabled {
		stats.NewMemoryReporter()
		hostname, _ := os.Hostname()
		prefix := strings.Replace(*statsPrefix, "$hostname", strings.Replace(hostname, ".", "_", -1), -1)
		stats.NewGraphite(prefix, *statsAddr, *statsInterval, *statsBufferSize)
	} else {
		stats.NewDevnull()
	}

	if *tsdbStatsEnabled {
		err := usage.Init(*tsdbStatsAddr)
		if err != nil {
			log.Fatalf("failed to initialize usage stats. %s", err.Error())
		}
	}

	_, traceCloser, err := util.GetTracer(app, *tracingEnabled, *tracingAddr)
	if err != nil {
		log.Fatalf("Could not initialize jaeger tracer: %s", err.Error())
	}
	defer traceCloser.Close()

	publisher := kafka.New(*broker)
	if publisher == nil {
		publish.Init(nil)
	} else {
		publish.Init(publisher)
	}

	if err := graphite.Init(*graphiteURL); err != nil {
		log.Fatalf(err.Error())
	}
	if err := metrictank.Init(*metrictankURL); err != nil {
		log.Fatalf(err.Error())
	}

	inputs := make([]Stoppable, 0)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	api := api.New(*authPlugin, app)
	initRoutes(api)

	log.Infof("Starting %v ...", app)
	done := make(chan struct{})
	inputs = append(inputs, api.Start(), carbon.InitCarbon())
	go handleShutdown(done, interrupt, inputs)
	log.Infof("%v Started", app)
	<-done
}

type Stoppable interface {
	Stop()
}

func handleShutdown(done chan struct{}, interrupt chan os.Signal, inputs []Stoppable) {
	<-interrupt
	log.Infoln("shutdown started.")
	var wg sync.WaitGroup
	for _, input := range inputs {
		wg.Add(1)
		go func(plugin Stoppable) {
			plugin.Stop()
			wg.Done()
		}(input)
	}

	complete := make(chan struct{})

	go func() {
		wg.Wait()
		close(complete)
	}()

	timer := time.NewTimer(time.Minute * 2)
	select {
	case <-timer.C:
		log.Errorln("shutdown taking too long, giving up waiting on plugins")
	case <-complete:
		log.Infof("shutdown complete")
	}
	close(done)
}

func initRoutes(a *api.Api) {
	a.Router.Use(api.RequestStats())
	a.Router.Post("/metrics/delete", a.Auth(), metrictank.MetrictankProxy("/metrics/delete"))
	a.Router.Get("/metrics/index.json", a.Auth(), metrictank.MetrictankProxy("/metrics/index.json"))
	a.Router.Get("/graphite/metrics/index.json", a.Auth(), metrictank.MetrictankProxy("/metrics/index.json"))
	a.Router.Any("/prometheus/*", a.Auth(), metrictank.PrometheusProxy)
	a.Router.Any("/graphite/*", a.Auth(), graphite.GraphiteProxy)
	a.Router.Post("/metrics", a.Auth(), ingest.Metrics)
	a.Router.Post("/datadog/api/v1/series", a.Auth(), ingest.DataDogMTWrite)
	a.Router.Post("/opentsdb/api/put", a.Auth(), ingest.OpenTSDBWrite)
	a.Router.Any("/prometheus/write", a.Auth(), ingest.PrometheusMTWrite)
}
