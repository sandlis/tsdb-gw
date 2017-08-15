package api

import (
	"crypto/tls"
	"flag"
	"net"
	"net/http"
	"os"

	"github.com/gorilla/handlers"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/macaron.v1"
)

var (
	addr     = flag.String("addr", "localhost:80", "http service address")
	ssl      = flag.Bool("ssl", false, "use https")
	certFile = flag.String("cert-file", "", "SSL certificate file")
	keyFile  = flag.String("key-file", "", "SSL key file")
	adminKey = flag.String("admin-key", "not_very_secret_key", "Admin Secret Key")
)

type Api struct {
	l    net.Listener
	done chan struct{}
}

func InitApi() *Api {
	if *ssl && (*certFile == "" || *keyFile == "") {
		log.Fatal(4, "cert-file and key-file must be set when using SSL")
	}

	m := macaron.New()
	m.Use(macaron.Recovery())
	m.Use(macaron.Renderer())

	InitRoutes(m, *adminKey)
	// define our own listner so we can call Close on it
	l, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatal(4, err.Error())
	}
	a := &Api{
		l:    l,
		done: make(chan struct{}),
	}

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

func InitRoutes(m *macaron.Macaron, adminKey string) {
	m.Use(GetContextHandler())
	m.Use(RequestStats())

	m.Get("/", index)
	m.Post("/metrics/delete", Auth(adminKey), MetrictankProxy)
	m.Post("/metrics", Auth(adminKey), Metrics)
	m.Post("/events", Auth(adminKey), Events)
	m.Any("/graphite/*", Auth(adminKey), GraphiteProxy)
	m.Any("/elasticsearch/*", Auth(adminKey), ElasticsearchProxy)
}

func index(ctx *macaron.Context) {
	ctx.JSON(200, "ok")
}
