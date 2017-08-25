package graphite

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"

	opentracing "github.com/opentracing/opentracing-go"
	"github.com/raintank/tsdb-gw/util"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/macaron.v1"
)

var (
	GraphiteUrl  *url.URL
	WorldpingUrl *url.URL
	wpProxy      httputil.ReverseProxy
	gProxy       httputil.ReverseProxy

	worldpingHack bool
)

type proxyRetryTransport struct {
	transport http.RoundTripper
}

func NewProxyRetrytransport() *proxyRetryTransport {
	return &proxyRetryTransport{
		transport: http.DefaultTransport,
	}
}

func (t *proxyRetryTransport) RoundTrip(outreq *http.Request) (*http.Response, error) {
	span := opentracing.SpanFromContext(outreq.Context())
	span = span.Tracer().StartSpan("graphite_rt", opentracing.ChildOf(span.Context()))
	defer span.Finish()

	carrier := opentracing.HTTPHeadersCarrier(outreq.Header)
	err := opentracing.GlobalTracer().Inject(span.Context(), opentracing.HTTPHeaders, carrier)
	if err != nil {
		log.Error(3, "CLU failed to inject span into headers: %s", err)
	}

	attempts := 0
	var res *http.Response
	hasBody := false
	var body []byte
	if outreq.Body != nil {
		body, err = ioutil.ReadAll(outreq.Body)
		if err != nil {
			return res, err
		}
		hasBody = true
	}

	for {
		attempts++
		if hasBody {
			outreq.Body = ioutil.NopCloser(bytes.NewBuffer(body))
		}
		attempt_span := span.Tracer().StartSpan(fmt.Sprintf("graphite_rt_attempt%d", attempts), opentracing.ChildOf(span.Context()))
		res, err = t.transport.RoundTrip(outreq)
		attempt_span.Finish()
		if err == nil {
			break
		}

		if attempts <= 3 {
			log.Info("graphiteProxy: request failed, will retry. %s", err)
		} else {
			log.Error(3, "graphiteProxy: request failed 3 times. Giving up. %s", err)
			break
		}
	}

	span.SetTag("attempts", attempts)
	return res, err
}

func Init(graphiteUrl, worldpingUrl string) error {
	var err error
	GraphiteUrl, err = url.Parse(graphiteUrl)
	if err != nil {
		return err
	}
	if worldpingUrl != "" {
		worldpingHack = true
		WorldpingUrl, err = url.Parse(worldpingUrl)
		if err != nil {
			return err
		}

		wpProxy.Director = func(req *http.Request) {
			req.URL.Scheme = WorldpingUrl.Scheme
			req.URL.Host = WorldpingUrl.Host
		}
	}

	gProxy.Director = func(req *http.Request) {
		req.URL.Scheme = GraphiteUrl.Scheme
		req.URL.Host = GraphiteUrl.Host
	}
	gProxy.Transport = NewProxyRetrytransport()

	return nil
}

func Proxy(orgId int, c *macaron.Context) {
	proxyPath := c.Params("*")

	// check if this is a special raintank_db c.Req.Requests then proxy to the worldping-api service.
	if worldpingHack && proxyPath == "metrics/find" && c.Req.Method == "GET" {
		query := c.Req.Request.FormValue("query")
		if strings.HasPrefix(query, "raintank_db") {
			c.Req.Request.URL.Path = util.JoinUrlFragments(WorldpingUrl.Path, "/api/graphite/"+proxyPath)
			wpProxy.ServeHTTP(c.Resp, c.Req.Request)
			return
		}
	}

	c.Req.Request.Header.Del("X-Org-Id")
	c.Req.Request.Header.Add("X-Org-Id", strconv.FormatInt(int64(orgId), 10))
	c.Req.Request.URL.Path = util.JoinUrlFragments(GraphiteUrl.Path, proxyPath)
	gProxy.ServeHTTP(c.Resp, c.Req.Request)
}
