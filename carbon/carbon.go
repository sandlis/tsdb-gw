package carbon

import (
	"bufio"
	"bytes"
	"flag"
	"io"
	"net"
	"sync"
	"time"

	"github.com/lomik/go-carbon/persister"
	m20 "github.com/metrics20/go-metrics20/carbon20"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/tsdb-gw/auth"
	"github.com/raintank/tsdb-gw/metric_publish"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

var (
	metricsReceived          = stats.NewCounter32("metrics.carbon.received")
	metricsValid             = stats.NewCounter32("metrics.carbon.valid")
	metricsRejected          = stats.NewCounter32("metrics.carbon.rejected")
	metricsFailed            = stats.NewCounter32("metrics.carbon.failed")
	metricsDroppedBufferFull = stats.NewCounter32("metrics.carbon.dropped_buffer_full")
	metricsDroppedAuthFail   = stats.NewCounter32("metrics.carbon.dropped_auth_fail")

	carbonConnections = stats.NewGauge32("carbon.connections")

	Enabled           bool
	addr              string
	schemasConf       string
	concurrency       int
	bufferSize        int
	flushInterval     time.Duration
	nonBlockingBuffer bool
	authPlugin        string

	metricPool = NewMetricDataPool()
)

func init() {
	flag.StringVar(&addr, "carbon-addr", "0.0.0.0:2003", "listen address for carbon input")
	flag.BoolVar(&Enabled, "carbon-enabled", false, "enable carbon input")
	flag.StringVar(&schemasConf, "schemas-file", "/etc/storage-schemas.conf", "path to carbon storage-schemas.conf file")
	flag.DurationVar(&flushInterval, "carbon-flush-interval", time.Second, "maximum time between flushs to kafka")
	flag.IntVar(&concurrency, "carbon-concurrency", 1, "number of goroutines for handling metrics")
	flag.IntVar(&bufferSize, "carbon-buffer-size", 100000, "number of metrics to hold in an input buffer. Once this buffer fills metrics will be dropped")
	flag.BoolVar(&nonBlockingBuffer, "carbon-non-blocking-buffer", false, "dont block trying to write to the input buffer, just drop metrics.")
	flag.StringVar(&authPlugin, "carbon-auth-plugin", "file", "auth plugin to use. (grafana|file)")
}

type Carbon struct {
	udp        net.Conn
	tcp        net.Listener
	listenWg   sync.WaitGroup
	schemas    persister.WhisperSchemas
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

	log.Info("Carbon input listening on %s", addr)
	c.buf = make(chan []byte, bufferSize)

	schemas, err := getSchemas(schemasConf)
	if err != nil {
		log.Fatal(4, "failed to load schemas config. %s", err)
	}
	c.schemas = schemas

	laddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal(4, "failed to resolve TCP address. %s", err)
	}
	l, err := net.ListenTCP("tcp", laddr)
	if err != nil {
		log.Fatal(4, "failed to listen on TCP address. %s", err)
	}
	c.tcp = l

	udp_addr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatal(4, "failed to resolve UDP address. %s", err)
	}
	udp_conn, err := net.ListenUDP("udp", udp_addr)
	if err != nil {
		log.Fatal(4, "failed to listen on UDP address. %s", err)
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
	log.Info("listening on %v", c.tcp.Addr())
	shutdown := make(chan struct{})
	var wg sync.WaitGroup
	for {
		// Listen for an incoming connection.
		conn, err := c.tcp.Accept()
		if err != nil {
			log.Info("listener error. %v", err)
			break
		}
		// Handle connections in a new goroutine.
		wg.Add(1)
		go c.handleConn(conn, shutdown, &wg)
	}
	close(shutdown)
	wg.Wait()
	log.Info("TCP listener has shutdown.")
	c.listenWg.Done()
	return
}

func (c *Carbon) handleConn(conn net.Conn, shutdown chan struct{}, wg *sync.WaitGroup) {
	carbonConnections.Inc()
	if conn.RemoteAddr() != nil {
		log.Info("handling connection from %s", conn.RemoteAddr().String())
	}
	defer func() {
		carbonConnections.Dec()
		if conn.RemoteAddr() != nil {
			log.Info("connection from %s ended", conn.RemoteAddr().String())
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
				log.Error(3, err.Error())
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
					log.Debug("metric dropped due to full buffer")
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
			err := metric_publish.Publish(buf)
			if err != nil {
				log.Error(3, "failed to publish metrics. %s", err)
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
				log.Debug("packet rejected with error. %s - %s", err, b)
				metricsRejected.Inc()
				continue
			}

			parts := bytes.SplitN(b, []byte("."), 2)
			user, err := c.authPlugin.Auth(string(parts[0]))
			if err != nil {
				log.Debug("invalid auth key. %s", b)
				metricsDroppedAuthFail.Inc()
				continue
			}
			md, err := parseMetric(parts[1], c.schemas, user.OrgId)
			if err != nil {
				log.Error(3, "could not parse metric. %s", err)
				metricsRejected.Inc()
				continue
			}
			buf = append(buf, md)
		}
	}
}

type MetricDataPool struct {
	pool sync.Pool
}

func NewMetricDataPool() *MetricDataPool {
	return &MetricDataPool{pool: sync.Pool{
		New: func() interface{} { return new(schema.MetricData) },
	}}
}

func (b *MetricDataPool) Get() *schema.MetricData {
	return b.pool.Get().(*schema.MetricData)
}

func (b *MetricDataPool) Put(m *schema.MetricData) {
	b.pool.Put(m)
}
