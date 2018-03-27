package api

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/stats"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
	otLog "github.com/opentracing/opentracing-go/log"
	"github.com/raintank/tsdb-gw/auth"
	"github.com/raintank/tsdb-gw/usage"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/macaron.v1"
)

type Context struct {
	*macaron.Context
	*auth.User
}

type TracingResponseWriter struct {
	macaron.ResponseWriter
	errBody []byte // the body in case it is an error
}

func (rw *TracingResponseWriter) Write(b []byte) (int, error) {
	if rw.ResponseWriter.Status() >= 400 {
		rw.errBody = make([]byte, len(b))
		copy(rw.errBody, b)
	}
	return rw.ResponseWriter.Write(b)
}

func GetContextHandler() macaron.Handler {
	return func(c *macaron.Context) {
		ctx := &Context{
			Context: c,
			User:    &auth.User{},
		}
		c.Map(ctx)
	}
}

func RequireAdmin() macaron.Handler {
	return func(ctx *Context) {
		if !ctx.IsAdmin {
			ctx.JSON(403, "Permision denied")
		}
	}
}

func (a *Api) Auth() macaron.Handler {
	return func(ctx *Context) {
		username, key, ok := ctx.Req.BasicAuth()
		if !ok {
			// no basicAuth, but we also need to check for a Bearer Token
			header := ctx.Req.Header.Get("Authorization")
			parts := strings.SplitN(header, " ", 2)
			if len(parts) == 2 && parts[0] == "Bearer" {
				key = parts[1]
				username = "api_key"
			}
		}

		if key == "" {
			key = ctx.Req.Header.Get("Dd-Api-Key")
			if key == "" {
				ctx.JSON(401, "Unauthorized")
				return
			}
			username = "api_key"
		}

		user, err := a.authPlugin.Auth(username, key)
		if err != nil {
			if err == auth.ErrInvalidCredentials || err == auth.ErrInvalidOrgId || err == auth.ErrInvalidInstanceID {
				ctx.JSON(401, err.Error())
				return
			}
			log.Error(3, "failed to perform authentication: %q", err.Error())
			ctx.JSON(500, err.Error())
			return
		}

		// allow admin users to impersonate other orgs.
		if user.IsAdmin {
			header := ctx.Req.Header.Get("X-Tsdb-Org")
			if header != "" {
				orgId, err := strconv.ParseInt(header, 10, 64)
				if err == nil && orgId != 0 {
					user.ID = int(orgId)
				}
			}
		}
		ctx.User = user
	}
}

type requestStats struct {
	sync.Mutex
	responseCounts    map[string]map[int]*stats.Counter32
	latencyHistograms map[string]*stats.LatencyHistogram15s32
	sizeMeters        map[string]*stats.Meter32
}

func (r *requestStats) PathStatusCount(ctx *Context, path string, status int) {
	metricKey := fmt.Sprintf("api.request.%s.status.%d", path, status)
	r.Lock()
	p, ok := r.responseCounts[path]
	if !ok {
		p = make(map[int]*stats.Counter32)
		r.responseCounts[path] = p
	}
	c, ok := p[status]
	if !ok {
		c = stats.NewCounter32(metricKey)
		p[status] = c
	}
	r.Unlock()
	c.Inc()
	usage.LogRequest(ctx.ID, metricKey)
}

func (r *requestStats) PathLatency(ctx *Context, path string, dur time.Duration) {
	r.Lock()
	p, ok := r.latencyHistograms[path]
	if !ok {
		p = stats.NewLatencyHistogram15s32(fmt.Sprintf("api.request.%s", path))
		r.latencyHistograms[path] = p
	}
	r.Unlock()
	p.Value(dur)
}

func (r *requestStats) PathSize(ctx *Context, path string, size int) {
	r.Lock()
	p, ok := r.sizeMeters[path]
	if !ok {
		p = stats.NewMeter32(fmt.Sprintf("api.request.%s.size", path), false)
		r.sizeMeters[path] = p
	}
	r.Unlock()
	p.Value(size)
}

// RequestStats returns a middleware that tracks request metrics.
func RequestStats() macaron.Handler {
	stats := requestStats{
		responseCounts:    make(map[string]map[int]*stats.Counter32),
		latencyHistograms: make(map[string]*stats.LatencyHistogram15s32),
		sizeMeters:        make(map[string]*stats.Meter32),
	}

	return func(ctx *Context) {
		start := time.Now()
		rw := ctx.Resp.(macaron.ResponseWriter)
		// call next handler. This will return after all handlers
		// have completed and the request has been sent.
		ctx.Next()
		status := rw.Status()
		path := pathSlug(ctx.Req.URL.Path)
		stats.PathStatusCount(ctx, path, status)
		stats.PathLatency(ctx, path, time.Since(start))
		// only record the request size if the request succeeded.
		if status < 300 {
			stats.PathSize(ctx, path, rw.Size())
		}
	}
}

func pathSlug(p string) string {
	slug := strings.TrimPrefix(path.Clean(p), "/")
	if slug == "" {
		slug = "root"
	}
	return strings.Replace(strings.Replace(slug, "/", "_", -1), ".", "_", -1)
}

func Tracer() macaron.Handler {
	return func(macCtx *macaron.Context) {
		tracer := opentracing.GlobalTracer()
		path := pathSlug(macCtx.Req.URL.Path)
		spanCtx, _ := tracer.Extract(opentracing.HTTPHeaders, opentracing.HTTPHeadersCarrier(macCtx.Req.Header))
		span := tracer.StartSpan("HTTP "+macCtx.Req.Method+" "+path, ext.RPCServerOption(spanCtx))

		ext.HTTPMethod.Set(span, macCtx.Req.Method)
		ext.HTTPUrl.Set(span, macCtx.Req.URL.String())
		ext.Component.Set(span, "tsdb-gw/api")

		macCtx.Req = macaron.Request{
			Request: macCtx.Req.WithContext(opentracing.ContextWithSpan(macCtx.Req.Context(), span)),
		}
		macCtx.Resp = &TracingResponseWriter{
			ResponseWriter: macCtx.Resp,
		}
		macCtx.MapTo(macCtx.Resp, (*http.ResponseWriter)(nil))

		rw := macCtx.Resp.(*TracingResponseWriter)
		// call next handler. This will return after all handlers
		// have completed and the request has been sent.
		macCtx.Next()
		status := rw.Status()
		ext.HTTPStatusCode.Set(span, uint16(status))
		if status >= 200 && status < 300 {
			span.SetTag("http.size", rw.Size())
		}
		if status >= 400 {
			span.LogFields(otLog.Error(errors.New(string(rw.errBody))))
			if status >= 500 {
				ext.Error.Set(span, true)
			}
		}
		span.Finish()
	}
}
