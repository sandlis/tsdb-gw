package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strings"

	"github.com/gorilla/handlers"
	"github.com/grafana/globalconf"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/tsdb-gw/api"
	"github.com/raintank/tsdb-gw/elasticsearch"
	"github.com/raintank/tsdb-gw/event_publish"
	"github.com/raintank/tsdb-gw/graphite"
	"github.com/raintank/tsdb-gw/metric_publish"
	"github.com/raintank/tsdb-gw/metrictank"
	"github.com/raintank/tsdb-gw/usage"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/macaron.v1"
)

var (
	GitHash     = "(none)"
	showVersion = flag.Bool("version", false, "print version string")
	logLevel    = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
	confFile    = flag.String("config", "/etc/raintank/tsdb.ini", "configuration file path")

	broker = flag.String("kafka-tcp-addr", "localhost:9092", "kafka tcp address for metrics")

	addr     = flag.String("addr", "localhost:80", "http service address")
	ssl      = flag.Bool("ssl", false, "use https")
	certFile = flag.String("cert-file", "", "SSL certificate file")
	keyFile  = flag.String("key-file", "", "SSL key file")

	statsEnabled    = flag.Bool("stats-enabled", false, "enable sending graphite messages for instrumentation")
	statsPrefix     = flag.String("stats-prefix", "tsdb-gw.stats.default.$hostname", "stats prefix (will add trailing dot automatically if needed)")
	statsAddr       = flag.String("stats-addr", "localhost:2003", "graphite address")
	statsInterval   = flag.Int("stats-interval", 10, "interval in seconds to send statistics")
	statsBufferSize = flag.Int("stats-buffer-size", 20000, "how many messages (holding all measurements from one interval) to buffer up in case graphite endpoint is unavailable.")

	graphiteUrl      = flag.String("graphite-url", "http://localhost:8080", "graphite-api address")
	metrictankUrl    = flag.String("metrictank-url", "http://localhost:6060", "metrictank address")
	worldpingUrl     = flag.String("worldping-url", "http://localhost/", "worldping-api address")
	elasticsearchUrl = flag.String("elasticsearch-url", "http://localhost:9200", "elasticsearch server address")
	esIndex          = flag.String("es-index", "events", "elasticsearch index name")

	tsdbStatsEnabled = flag.Bool("tsdb-stats-enabled", false, "enable collecting usage stats")
	tsdbStatsAddr    = flag.String("tsdb-stats-addr", "localhost:2004", "tsdb-usage server address")

	adminKey = flag.String("admin-key", "not_very_secret_key", "Admin Secret Key")
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

	if *ssl && (*certFile == "" || *keyFile == "") {
		log.Fatal(4, "cert-file and key-file must be set when using SSL")
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

	metric_publish.Init(*broker)
	event_publish.Init(*broker)

	m := macaron.New()
	m.Use(macaron.Recovery())
	m.Use(macaron.Renderer())

	api.InitRoutes(m, *adminKey)

	if err := graphite.Init(*graphiteUrl, *worldpingUrl); err != nil {
		log.Fatal(4, err.Error())
	}
	if err := metrictank.Init(*metrictankUrl); err != nil {
		log.Fatal(4, err.Error())
	}
	if err := elasticsearch.Init(*elasticsearchUrl, *esIndex); err != nil {
		log.Fatal(4, err.Error())
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)

	log.Info("starting up")
	// define our own listner so we can call Close on it
	l, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	done := make(chan struct{})
	go handleShutdown(done, interrupt, l)

	// write Request logs in Apache Common Log Format
	loggedRouter := handlers.LoggingHandler(os.Stdout, m)
	srv := http.Server{
		Addr:    *addr,
		Handler: loggedRouter,
	}
	if *ssl {
		cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
		if err != nil {
			log.Fatal(4, "Fail to start server: %v", err)
		}
		srv.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{"http/1.1"},
		}
		tlsListener := tls.NewListener(l, srv.TLSConfig)
		err = srv.Serve(tlsListener)
	} else {
		err = srv.Serve(l)
	}

	if err != nil {
		log.Info(err.Error())
	}
	<-done
}

func handleShutdown(done chan struct{}, interrupt chan os.Signal, l net.Listener) {
	<-interrupt
	log.Info("shutdown started.")
	l.Close()
	close(done)
}
