package main

import (
	"flag"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/raintank/tsdb-gw/persister"
	"github.com/raintank/tsdb-gw/util"
	log "github.com/sirupsen/logrus"
)

/*
Application: persister-gw

persister-gw is stores and persists metrics to a configure publisher backend. It will store metrics
*/

func indexHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("ok"))
}

func main() {
	cfg := &persister.Config{}
	util.RegisterFlags(cfg)
	flag.Parse()
	util.InitLogger()

	p, err := persister.NewPersister(cfg)
	if err != nil {
		log.Fatalf("failed to start: %v", err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/", indexHandler)
	r.HandleFunc("/persist", p.PersistHandler)
	r.Handle("/metrics", promhttp.Handler())

	loggedRouter := handlers.CombinedLoggingHandler(os.Stdout, r)

	go p.Push(make(chan struct{}))

	srv := &http.Server{
		Addr:         "0.0.0.0:9001",
		WriteTimeout: time.Second * 15,
		ReadTimeout:  time.Second * 15,
		IdleTimeout:  time.Second * 60,
		Handler:      loggedRouter,
	}

	srv.ListenAndServe()
}
