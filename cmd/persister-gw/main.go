package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/grafana/globalconf"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/raintank/tsdb-gw/persister"
	"github.com/raintank/tsdb-gw/util"
	log "github.com/sirupsen/logrus"
)

//Application: persister-gw

var (
	app         = "persister-gw"
	GitHash     = "(none)"
	showVersion = flag.Bool("version", false, "print version string")
	confFile    = flag.String("config", "/etc/gw/persister-gw.ini", "configuration file path")
	addr        = flag.String("addr", "0.0.0.0:80", "http service address")
)

func indexHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

func main() {
	cfg := &persister.Config{}
	util.RegisterFlags(cfg)
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
		fmt.Printf("%s (built with %s, git hash %s)\n", app, runtime.Version(), GitHash)
		return
	}

	util.InitLogger()

	p, err := persister.New(cfg)
	if err != nil {
		log.Fatalf("failed to start: %v", err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/", indexHandler)
	r.HandleFunc("/persist", p.PersistHandler)
	r.HandleFunc("/remove", p.RemoveRowsHandler)
	r.HandleFunc("/index", p.IndexHandler)
	r.Handle("/metrics", promhttp.Handler())

	loggedRouter := handlers.CombinedLoggingHandler(os.Stdout, r)

	go p.Push(make(chan struct{}))

	srv := &http.Server{
		Addr:         *addr,
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      loggedRouter,
	}

	srv.ListenAndServe()
}
