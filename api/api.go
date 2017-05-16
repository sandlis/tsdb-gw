package api

import (
	"github.com/raintank/met"
	"gopkg.in/macaron.v1"
)

var (
	requestLatency met.Timer
	requestSize    met.Meter
	response2xx    met.Count
	response3xx    met.Count
	response4xx    met.Count
	response5xx    met.Count
)

func InitRoutes(metrics met.Backend, m *macaron.Macaron, adminKey string) {
	m.Use(GetContextHandler())
	m.Use(RequestStats())

	m.Get("/", index)
	m.Post("/metrics/delete", Auth(adminKey), MetrictankProxy)
	m.Post("/metrics", Auth(adminKey), Metrics)
	m.Post("/events", Auth(adminKey), Events)
	m.Any("/graphite/*", Auth(adminKey), GraphiteProxy)
	m.Any("/elasticsearch/*", Auth(adminKey), ElasticsearchProxy)

	requestLatency = metrics.NewTimer("request.duration", 0)
	requestSize = metrics.NewMeter("request.size", 0)
	response2xx = metrics.NewCount("request.2xx")
	response3xx = metrics.NewCount("request.3xx")
	response4xx = metrics.NewCount("request.4xx")
	response5xx = metrics.NewCount("request.5xx")
}

func index(ctx *macaron.Context) {
	ctx.JSON(200, "ok")
}
