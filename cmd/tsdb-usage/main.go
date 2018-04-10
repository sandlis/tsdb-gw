package main

import (
	"bufio"
	"bytes"
	"flag"
	"io"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/golang/glog"
	"github.com/grafana/globalconf"
	"github.com/raintank/tsdb-gw/publish"
	"github.com/raintank/tsdb-gw/publish/kafka"
	"github.com/raintank/tsdb-gw/usage"
)

var (
	broker   = flag.String("kafka-tcp-addr", "localhost:9092", "kafka tcp address for metrics")
	addr     = flag.String("addr", "localhost:2004", "listen address")
	confFile = flag.String("config", "/etc/gw/tsdb-usage.ini", "configuration file path")
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
		EnvPrefix: "TU_",
	})
	if err != nil {
		glog.Fatalf("error with configuration file: %s", err)
	}
	conf.ParseAll()

	glog.Info("tsdb-usage starting up")

	// initialize our publisher that sends metrics to Kafka
	publish.Init(kafka.New(*broker))

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	// define our own listener
	l, err := net.Listen("tcp", *addr)
	if err != nil {
		glog.Fatal(4, err.Error())
	}
	glog.Infof("tsdb-usage listening on %s", *addr)

	// cleanly shutdown when we are asked to
	go handleShutdown(interrupt, l)
	Init()
	StatsRun()
	listen(l)
}

func handleShutdown(interrupt chan os.Signal, l net.Listener) {
	<-interrupt
	glog.Info("shutdown started.")
	l.Close()
}

func listen(l net.Listener) {
	shutdown := make(chan struct{})
	var wg sync.WaitGroup
	for {
		// Listen for an incoming connection.
		conn, err := l.Accept()
		if err != nil {
			glog.Infof("listener error. %v", err)
			break
		}
		// Handle connections in a new goroutine.
		wg.Add(1)
		go handleRequest(conn, shutdown, &wg)
	}
	close(shutdown)
	wg.Wait()
	glog.Info("listener has shutdown.")
	return
}

func handleRequest(conn net.Conn, shutdown chan struct{}, wg *sync.WaitGroup) {
	defer func() {
		glog.Infof("connection from %s ended", conn.RemoteAddr().String())
		wg.Done()
	}()

	// if shutdown is received, close the connection.
	go func() {
		<-shutdown
		conn.Close()
	}()

	glog.Infof("new connection from %s", conn.RemoteAddr().String())
	r := bufio.NewReaderSize(conn, 4096)
	var org int64
	for {
		conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		buf, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				glog.Infof("EOF from %s", conn.RemoteAddr().String())
			} else {
				glog.Errorf("connect error from %s. %v", conn.RemoteAddr().String(), err)
			}
			conn.Close()
			return
		}
		eType := usage.EventType(buf[0])
		if !(eType == usage.DataPointReceived || eType == usage.ApiRequest) {
			glog.V(4).Infof("unknown event type %c", eType)
			continue
		}
		// the data payload is in the form <org>.<metric>
		data := bytes.SplitN(buf[1:], []byte{'.'}, 2)
		org, err = strconv.ParseInt(string(data[0]), 10, 32)
		if err != nil {
			glog.Errorf("invalid data. could not parse org. %v", err)
			continue
		}
		Record(&usage.Event{
			Org:   int32(org),
			ID:    string(data[1]),
			EType: eType,
		})
	}
}
