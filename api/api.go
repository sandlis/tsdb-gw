package api

import (
	"gopkg.in/macaron.v1"
)

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
