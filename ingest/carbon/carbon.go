package carbon

import (
	"bufio"
	"bytes"
	"flag"
	"io"
	"net"
	"sync"
	"time"

	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/stats"
	m20 "github.com/metrics20/go-metrics20/carbon20"
	"github.com/raintank/tsdb-gw/auth"
	"github.com/raintank/tsdb-gw/publish"
	"github.com/raintank/tsdb-gw/util"
	log "github.com/sirupsen/logrus"
	"gopkg.in/raintank/schema.v1"
)

var (
	metricsReceived          = stats.NewCounterRate32("metrics.carbon.received")
	metricsValid             = stats.NewCounterRate32("metrics.carbon.valid")
	metricsRejected          = stats.NewCounterRate32("metrics.carbon.rejected")
	metricsFailed            = stats.NewCounterRate32("metrics.carbon.failed")
	metricsDroppedBufferFull = stats.NewCounterRate32("metrics.carbon.dropped_buffer_full")
	metricsDroppedAuthFail   = stats.NewCounterRate32("metrics.carbon.dropped_auth_fail")

	carbonConnections = stats.NewGauge32("carbon.connections")

	Enabled           bool
	addr              string
	concurrency       int
	bufferSize        int
	flushInterval     time.Duration
	nonBlockingBuffer bool
	authPlugin        string

	metricPool = util.NewMetricDataPool()
)

func init() {
	flag.BoolVar(&Enabled, "carbon-enabled", false, "enable carbon input")
	flag.StringVar(&addr, "carbon-addr", "0.0.0.0:2003", "listen address for carbon input")
	flag.StringVar(&authPlugin, "carbon-auth-plugin", "file", "auth plugin to use. (grafana|file)")
	flag.DurationVar(&flushInterval, "carbon-flush-interval", time.Second, "maximum time between flushs to kafka")
	flag.IntVar(&concurrency, "carbon-concurrency", 1, "number of goroutines for handling metrics")
	flag.IntVar(&bufferSize, "carbon-buffer-size", 100000, "number of metrics to hold in an input buffer. Once this buffer fills metrics will be dropped")
	flag.BoolVar(&nonBlockingBuffer, "carbon-non-blocking-buffer", false, "dont block trying to write to the input buffer, just drop metrics.")
}

type Carbon struct {
	udp        net.Conn
	tcp        net.Listener
	listenWg   sync.WaitGroup
	schemas    *conf.Schemas
	buf        chan []byte
	flushWg    sync.WaitGroup
	authPlugin auth.AuthPlugin
}

func InitCarbon() *Carbon {
	c := new(Carbon)
	if !Enabled {
		return c
	}

	c.authPlugin = auth.GetAuthPlugin(authPlugin)

	log.Infof("Carbon input listening on %s", addr)
	c.buf = make(chan []byte, bufferSize)

	laddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatalf("failed to resolve TCP address. %s", err)
	}
	l, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		log.Fatalf("failed to listen on TCP address. %s", err)
	}
	c.tcp = l

	udp_addr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatalf("failed to resolve UDP address. %s", err)
	}
	udp_conn, err := net.ListenUDP("udp", udp_addr)
	if err != nil {
		log.Fatalf("failed to listen on UDP address. %s", err)
	}
	c.udp = udp_conn

	// start acceting TCP connections
	c.listenWg.Add(1)
	go c.listen()

	c.listenWg.Add(1)
	// this chan will never be closed, but the UDP connection will be closed when Stop() is called.
	shutdown := make(chan struct{})
	go c.handleConn(c.udp, shutdown, &c.listenWg)

	for i := 0; i < concurrency; i++ {
		c.flushWg.Add(1)
		go c.flush()
	}

	return c
}

func (c *Carbon) Stop() {
	if !Enabled {
		return
	}
	c.tcp.Close()
	c.udp.Close()
	c.listenWg.Wait()
	// all listeners are closed, so we can close the input Buf
	close(c.buf)
	c.flushWg.Wait()
}

func (c *Carbon) listen() {
	log.Infof("listening on %v", c.tcp.Addr())
	shutdown := make(chan struct{})
	var wg sync.WaitGroup
	for {
		// Listen for an incoming connection.
		conn, err := c.tcp.Accept()
		if err != nil {
			log.Infof("listener error. %v", err)
			break
		}
		// Handle connections in a new goroutine.
		wg.Add(1)
		go c.handleConn(conn, shutdown, &wg)
	}
	close(shutdown)
	wg.Wait()
	log.Infoln("TCP listener has shutdown.")
	c.listenWg.Done()
	return
}

func (c *Carbon) handleConn(conn net.Conn, shutdown chan struct{}, wg *sync.WaitGroup) {
	carbonConnections.Inc()
	if conn.RemoteAddr() != nil {
		log.Infof("handling connection from %s", conn.RemoteAddr().String())
	}
	defer func() {
		carbonConnections.Dec()
		if conn.RemoteAddr() != nil {
			log.Infof("connection from %s ended", conn.RemoteAddr().String())
		}
		wg.Done()
	}()

	// if shutdown is received, close the connection.
	go func() {
		<-shutdown
		conn.Close()
	}()
	reader := bufio.NewReader(conn)
	for {
		if conn.RemoteAddr() != nil {
			conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
		}
		// note that we don't support lines longer than 4096B. that seems very reasonable..
		line, err := reader.ReadBytes('\n')

		if err != nil {
			if io.EOF != err {
				log.Errorln(err.Error())
			}
			break
		}
		if len(line) > 0 {
			metricsReceived.Inc()
			if nonBlockingBuffer {
				select {
				case c.buf <- line:
				default:
					metricsDroppedBufferFull.Inc()
					log.Debugln("metric dropped due to full buffer")
					// maybe we should just close the connection here
				}
			} else {
				c.buf <- line
			}
		}
	}
}

func (c *Carbon) flush() {
	defer c.flushWg.Done()
	buf := make([]*schema.MetricData, 0)

	ticker := time.NewTicker(flushInterval)
	for {
		select {
		case <-ticker.C:
			err := publish.Publish(buf)
			if err != nil {
				log.Errorf("failed to publish metrics. %s", err)
				metricsFailed.Add(len(buf))
				continue
			}
			metricsValid.Add(len(buf))
			for _, m := range buf {
				metricPool.Put(m)
			}
			buf = buf[0:0]
		case b, ok := <-c.buf:
			if !ok {
				return
			}
			_, _, _, err := m20.ValidatePacket(b, m20.StrictLegacy, m20.NoneM20)
			if err != nil {
				log.Debugf("packet rejected with error. %s - %s", err, b)
				metricsRejected.Inc()
				continue
			}

			parts := bytes.SplitN(b, []byte("."), 2)
			user, err := c.authPlugin.Auth("api_key", string(parts[0]))
			if err != nil {
				log.Debugf("invalid auth key. %s, reason: %v", parts[0], err)
				metricsDroppedAuthFail.Inc()
				continue
			}
			md, err := parseMetric(parts[1], c.schemas, user.ID)
			if err != nil {
				log.Errorf("could not parse metric %q: %s", string(parts[1]), err)
				metricsRejected.Inc()
				continue
			}
			buf = append(buf, md)
		}
	}
}
