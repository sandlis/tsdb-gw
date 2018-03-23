package api

import (
	"crypto/tls"
	"flag"
	"net"
	"net/http"
	"os"

	"github.com/gorilla/handlers"
	"github.com/raintank/tsdb-gw/auth"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/macaron.v1"
)

var (
	addr       = flag.String("addr", "localhost:80", "http service address")
	ssl        = flag.Bool("ssl", false, "use https")
	certFile   = flag.String("cert-file", "", "SSL certificate file")
	keyFile    = flag.String("key-file", "", "SSL key file")
	authPlugin = flag.String("api-auth-plugin", "grafana", "auth plugin to use. (grafana|file)")
)

type Api struct {
	l          net.Listener
	done       chan struct{}
	authPlugin auth.AuthPlugin
}

func InitApi() *Api {
	if *ssl && (*certFile == "" || *keyFile == "") {
		log.Fatal(4, "cert-file and key-file must be set when using SSL")
	}

	a := &Api{
		done:       make(chan struct{}),
		authPlugin: auth.GetAuthPlugin(*authPlugin),
	}

	m := macaron.New()
	m.Use(macaron.Recovery())
	m.Use(macaron.Renderer())
	m.Use(Tracer())

	// define our own listner so we can call Close on it
	l, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	a.l = l

	PrometheusMTInit()
	a.InitRoutes(m)

	// write Request logs in Apache Combined Log Format
	loggedRouter := handlers.CombinedLoggingHandler(os.Stdout, m)
	srv := http.Server{
		Addr:    *addr,
		Handler: loggedRouter,
	}
	go func() {
		defer close(a.done)
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
	}()
	return a
}

func (a *Api) Stop() {
	a.l.Close()
	<-a.done
}

func (a *Api) InitRoutes(m *macaron.Macaron) {
	m.Use(GetContextHandler())
	m.Use(RequestStats())
	m.Get("/", index)

	m.Post("/metrics/delete", a.Auth(), MetrictankProxy("/metrics/delete"))
	m.Post("/metrics", a.Auth(), Metrics)
	m.Get("/metrics/index.json", a.Auth(), MetrictankProxy("/metrics/index.json"))
	m.Get("/graphite/metrics/index.json", a.Auth(), MetrictankProxy("/metrics/index.json"))
	m.Any("/graphite/*", a.Auth(), GraphiteProxy)
	if *prometheusMTWriteEnabled {
		m.Any("/prometheus/write", a.Auth(), PrometheusMTWrite)
	}
	m.Any("/prometheus/*", a.Auth(), PrometheusProxy)
	m.Post("/opentsdb/api/put", a.Auth(), OpenTSDBWrite)
	m.Any("/api/prom/push", a.CortexAuth(), CortexWrite)
	m.Any("/api/prom/*", a.CortexAuth(), CortexProxy)
}

func index(ctx *macaron.Context) {
	ctx.JSON(200, "ok")
}
