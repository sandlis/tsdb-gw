package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/grafana/globalconf"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/raintank/tsdb-gw/api"
	"github.com/raintank/tsdb-gw/query/cortex"
	"github.com/raintank/tsdb-gw/util"
	log "github.com/sirupsen/logrus"
)

var (
	app         = "cortex-gw"
	GitHash     = "(none)"
	showVersion = flag.Bool("version", false, "print version string")
	confFile    = flag.String("config", "/etc/cortex-gw.ini", "configuration file path")
	authPlugin  = flag.String("api-auth-plugin", "grafana-instance", "auth plugin to use. (grafana-instance|file)")

	tracingEnabled = flag.Bool("tracing-enabled", false, "enable/disable distributed opentracing via jaeger")
	tracingAddr    = flag.String("tracing-addr", "localhost:6831", "address of the jaeger agent to send data to")
	metricsAddr    = flag.String("metrics-addr", ":8001", "http service address for the /metrics endpoint")
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
	initRoutes(api)

	ms := newMetricsServer(*metricsAddr)

	inputs := make([]Stoppable, 0)
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	log.Infof("Starting %v ...", app)
	done := make(chan struct{})
	inputs = append(inputs, api.Start(), ms)
	go handleShutdown(done, interrupt, inputs)
	log.Infof("%v Started", app)
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

// InitRoutes initializes the routes.
func initRoutes(a *api.Api) {
	a.Router.Any("/api/prom/push", a.PromStats("cortex-write"), a.Auth(), cortex.Write)
	a.Router.Any("/api/prom/*", a.PromStats("cortex-read"), a.Auth(), cortex.Proxy)
}

type metricsServer struct {
	srv *http.Server
}

func newMetricsServer(addr string) *metricsServer {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil {
			log.Fatalf("Failed to start metrics server: %v", err)
		}
	}()

	return &metricsServer{srv}
}

func (m *metricsServer) Stop() {
	m.srv.Close()
}
