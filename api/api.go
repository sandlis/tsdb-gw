package api

import (
	"crypto/tls"
	"flag"
	"net"
	"net/http"
	"os"

	"github.com/gorilla/handlers"
	"github.com/raintank/tsdb-gw/auth"
	log "github.com/sirupsen/logrus"
	"gopkg.in/macaron.v1"
)

var (
	addr     = flag.String("addr", "localhost:80", "http service address")
	ssl      = flag.Bool("ssl", false, "use https")
	certFile = flag.String("cert-file", "", "SSL certificate file")
	keyFile  = flag.String("key-file", "", "SSL key file")
)

type Api struct {
	l          net.Listener
	done       chan struct{}
	authPlugin auth.AuthPlugin
	Router     *macaron.Macaron
}

func New(authPlugin string, appName string) *Api {
	if *ssl && (*certFile == "" || *keyFile == "") {
		log.Fatal("cert-file and key-file must be set when using SSL")
	}

	a := &Api{
		done:       make(chan struct{}),
		authPlugin: auth.GetAuthPlugin(authPlugin),
	}

	// define our own listner so we can call Close on it
	l, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(err.Error())
	}
	a.l = l
	m := macaron.New()
	m.Use(macaron.Recovery())
	m.Use(macaron.Renderer())
	m.Use(Tracer(appName))
	a.InitRoutes(m)

	a.Router = m
	return a
}

func (a *Api) Start() *Api {
	// write Request logs in Apache Combined Log Format
	loggedRouter := handlers.CombinedLoggingHandler(os.Stdout, a.Router)
	srv := http.Server{
		Addr:    *addr,
		Handler: loggedRouter,
	}

	go func() {
		defer close(a.done)
		var err error
		if *ssl {
			cert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
			if err != nil {
				log.Fatalf("Fail to start server: %v", err)
			}
			srv.TLSConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
				NextProtos:   []string{"http/1.1"},
			}
			tlsListener := tls.NewListener(a.l, srv.TLSConfig)
			err = srv.Serve(tlsListener)
		} else {
			err = srv.Serve(a.l)
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

func index(ctx *macaron.Context) {
	ctx.JSON(200, "ok")
}

func (a *Api) InitRoutes(m *macaron.Macaron) {
	m.Use(GetContextHandler())
	m.Use(RequestStats())
	m.Get("/", index)
}
