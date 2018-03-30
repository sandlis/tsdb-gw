package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"

	"github.com/grafana/globalconf"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/raintank/tsdb-gw/api"
	"github.com/raintank/tsdb-gw/cortex"
	"github.com/raintank/tsdb-gw/util"
	log "github.com/sirupsen/logrus"
)

var (
	app         = "cortex-gw"
	GitHash     = "(none)"
	showVersion = flag.Bool("version", false, "print version string")
	confFile    = flag.String("config", "/etc/tsdb-gw/tsdb.ini", "configuration file path")
	authPlugin  = flag.String("api-auth-plugin", "grafana-instance", "auth plugin to use. (grafana-instance|file)")

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
		log.Fatalf("error with configuration file: %s", err)
		os.Exit(1)
	}
	conf.ParseAll()

	if *showVersion {
		fmt.Printf("cortex-gw (built with %s, git hash %s)\n", runtime.Version(), GitHash)
		return
	}
	util.InitLogger()

	_, traceCloser, err := util.GetTracer(app, *tracingEnabled, *tracingAddr)
	if err != nil {
		log.Fatal("Could not initialize jaeger tracer: %s", err.Error())
	}
	defer traceCloser.Close()

	if err := cortex.Init(); err != nil {
		log.Fatal("could not initialize cortex proxy: %s", err.Error())
	}
	api := api.New(*authPlugin, app)
	InitRoutes(api)

	inputs := make([]Stoppable, 0)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	log.Infoln("starting up")
	done := make(chan struct{})
	inputs = append(inputs, api.Start())
	go handleShutdown(done, interrupt, inputs)

	<-done
}

// Stoppable represents things that can be stopped.
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
	wg.Wait()
	close(done)
}

// InitRoutes initializes the routes.
func InitRoutes(a *api.Api) {
	a.Router.Any("/api/prom/push", a.PromStats("cortex-write"), a.Auth(), cortex.Write)
	a.Router.Any("/api/prom/*", a.PromStats("cortex-read"), a.Auth(), cortex.Proxy)
	a.Router.Get("/metrics", promhttp.Handler())
}
