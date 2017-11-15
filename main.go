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

	"github.com/grafana/globalconf"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/tsdb-gw/api"
	"github.com/raintank/tsdb-gw/carbon"
	"github.com/raintank/tsdb-gw/graphite"
	"github.com/raintank/tsdb-gw/metric_publish"
	"github.com/raintank/tsdb-gw/metrictank"
	"github.com/raintank/tsdb-gw/prometheus"
	"github.com/raintank/tsdb-gw/usage"
	"github.com/raintank/tsdb-gw/util"
	"github.com/raintank/worldping-api/pkg/log"
)

var (
	GitHash     = "(none)"
	showVersion = flag.Bool("version", false, "print version string")
	logLevel    = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	confFile    = flag.String("config", "/etc/raintank/tsdb.ini", "configuration file path")

	broker = flag.String("kafka-tcp-addr", "localhost:9092", "kafka tcp address for metrics")

	statsEnabled    = flag.Bool("stats-enabled", false, "enable sending graphite messages for instrumentation")
	statsPrefix     = flag.String("stats-prefix", "tsdb-gw.stats.default.$hostname", "stats prefix (will add trailing dot automatically if needed)")
	statsAddr       = flag.String("stats-addr", "localhost:2003", "graphite address")
	statsInterval   = flag.Int("stats-interval", 10, "interval in seconds to send statistics")
	statsBufferSize = flag.Int("stats-buffer-size", 20000, "how many messages (holding all measurements from one interval) to buffer up in case graphite endpoint is unavailable.")

	graphiteUrl   = flag.String("graphite-url", "http://localhost:8080", "graphite-api address")
	metrictankUrl = flag.String("metrictank-url", "http://localhost:6060", "metrictank address")

	tsdbStatsEnabled = flag.Bool("tsdb-stats-enabled", false, "enable collecting usage stats")
	tsdbStatsAddr    = flag.String("tsdb-stats-addr", "localhost:2004", "tsdb-usage server address")

	tracingEnabled = flag.Bool("tracing-enabled", false, "enable/disable distributed opentracing via jaeger")
	tracingAddr    = flag.String("tracing-addr", "localhost:6831", "address of the jaeger agent to send data to")
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
		log.Fatal(4, "error with configuration file: %s", err)
		os.Exit(1)
	}
	conf.ParseAll()

	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":true}`, *logLevel))
	// workaround for https://github.com/grafana/grafana/issues/4055
	switch *logLevel {
	case 0:
		log.Level(log.TRACE)
	case 1:
		log.Level(log.DEBUG)
	case 2:
		log.Level(log.INFO)
	case 3:
		log.Level(log.WARN)
	case 4:
		log.Level(log.ERROR)
	case 5:
		log.Level(log.CRITICAL)
	case 6:
		log.Level(log.FATAL)
	}

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
			log.Fatal(4, "failed to initialize usage stats. %s", err.Error())
		}
	}

	_, traceCloser, err := util.GetTracer(*tracingEnabled, *tracingAddr)
	if err != nil {
		log.Fatal(4, "Could not initialize jaeger tracer: %s", err.Error())
	}
	defer traceCloser.Close()

	metric_publish.Init(*broker)

	if err := graphite.Init(*graphiteUrl); err != nil {
		log.Fatal(4, err.Error())
	}
	if err := metrictank.Init(*metrictankUrl); err != nil {
		log.Fatal(4, err.Error())
	}
	inputs := make([]Stoppable, 0)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	log.Info("starting up")
	done := make(chan struct{})
	inputs = append(inputs, api.InitApi(), carbon.InitCarbon(), prometheus.InitPrometheusWriter())
	go handleShutdown(done, interrupt, inputs)

	<-done
}

type Stoppable interface {
	Stop()
}

func handleShutdown(done chan struct{}, interrupt chan os.Signal, inputs []Stoppable) {
	<-interrupt
	log.Info("shutdown started.")
	var wg sync.WaitGroup
	for _, input := range inputs {
		wg.Add(1)
		go func(plugin Stoppable) {
			plugin.Stop()
			wg.Done()
		}(input)
	}
	wg.Wait()
	close(done)
}
