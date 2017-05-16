package api

import (
	"encoding/base64"
	"strings"
	"time"

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
		}
		if key == "" {
			ctx.JSON(401, "Unauthorized")
			return
		}
		user, err := auth.Auth(adminKey, key)
		if err != nil {
			if err == auth.ErrInvalidApiKey {
				ctx.JSON(401, "Unauthorized")
				return
			}
			ctx.JSON(500, err)
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

// RequestStats returns a middleware that tracks request metrics.
func RequestStats() macaron.Handler {
	return func(ctx *macaron.Context) {
		start := time.Now()
		rw := ctx.Resp.(macaron.ResponseWriter)
		ctx.Next()

		status := rw.Status()
		switch {
		case status >= 200 && status < 300:
			response2xx.Inc(1)
		case status >= 300 && status < 400:
			response3xx.Inc(1)
		case status >= 400 && status < 500:
			response4xx.Inc(1)
		case status >= 500:
			response5xx.Inc(1)
		}
		requestLatency.Value(time.Since(start))
		requestSize.Value(int64(rw.Size()))
	}
}
