package api

import (
	"encoding/base64"
	"fmt"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/raintank/metrictank/stats"
	"github.com/raintank/raintank-apps/pkg/auth"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/macaron.v1"
)

type Context struct {
	*macaron.Context
	*auth.SignedInUser
}

func GetContextHandler() macaron.Handler {
	return func(c *macaron.Context) {
		ctx := &Context{
			Context:      c,
			SignedInUser: &auth.SignedInUser{},
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

func Auth(adminKey string) macaron.Handler {
	return func(ctx *Context) {
		key, err := getApiKey(ctx)
		if err != nil {
			ctx.JSON(401, "Invalid Authentication header.")
			return
		}
		if key == "" {
			ctx.JSON(401, "Unauthorized")
			return
		}
		user, err := auth.Auth(adminKey, key)
		if err != nil {
			if err == auth.ErrInvalidApiKey || err == auth.ErrInvalidOrgId {
				ctx.JSON(401, err.Error())
				return
			}
			ctx.JSON(500, err.Error())
			return
		}
		ctx.SignedInUser = user
	}
}

func getApiKey(c *Context) (string, error) {
	header := c.Req.Header.Get("Authorization")
	parts := strings.SplitN(header, " ", 2)
	if len(parts) == 2 && parts[0] == "Bearer" {
		key := parts[1]
		return key, nil
	}

	if len(parts) == 2 && parts[0] == "Basic" {
		decoded, err := base64.StdEncoding.DecodeString(parts[1])
		if err != nil {
			log.Warn("Unable to decode basic auth header.", err)
			return "", err
		}
		userAndPass := strings.SplitN(string(decoded), ":", 2)
		if userAndPass[0] == "api_key" {
			return userAndPass[1], nil
		}
	}

	return "", nil
}

type requestStats struct {
	sync.Mutex
	responseCounts    map[string]map[int]*stats.Counter32
	latencyHistograms map[string]*stats.LatencyHistogram15s32
	sizeMeters        map[string]*stats.Meter32
}

func (r *requestStats) PathStatusCount(path string, status int) {
	r.Lock()
	p, ok := r.responseCounts[path]
	if !ok {
		p = make(map[int]*stats.Counter32)
		r.responseCounts[path] = p
	}
	c, ok := p[status]
	if !ok {
		c = stats.NewCounter32(fmt.Sprintf("api.request.%s.status.%d", path, status))
		p[status] = c
	}
	r.Unlock()
	c.Inc()
}

func (r *requestStats) PathLatency(path string, dur time.Duration) {
	r.Lock()
	p, ok := r.latencyHistograms[path]
	if !ok {
		p = stats.NewLatencyHistogram15s32(fmt.Sprintf("api.request.%s", path))
		r.latencyHistograms[path] = p
	}
	r.Unlock()
	p.Value(dur)
}

func (r *requestStats) PathSize(path string, size int) {
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

	return func(ctx *macaron.Context) {
		start := time.Now()
		rw := ctx.Resp.(macaron.ResponseWriter)
		// call next handler. This will return after all handlers
		// have completed and the request has been sent.
		ctx.Next()
		status := rw.Status()
		path := pathSlug(ctx.Req.URL.Path)
		stats.PathStatusCount(path, status)
		stats.PathLatency(path, time.Since(start))
		// only record the request size if the request succeeded.
		if status < 300 {
			stats.PathSize(path, rw.Size())
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