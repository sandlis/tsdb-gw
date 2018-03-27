package api

import (
	"flag"

	"github.com/grafana/metrictank/conf"
	"github.com/raintank/tsdb-gw/metricpool"
	"github.com/raintank/worldping-api/pkg/log"
)

var (
	metricPool    = metricpool.NewMetricDataPool()
	schemas       *conf.Schemas
	schemaFile    = flag.String("api-schemas-file", "/etc/storage-schemas.conf", "path to carbon storage-schemas.conf file")
	ingestEnabled = flag.Bool("api-ingest", true, "enable api ingest for datadog/opentsdb/prometheus for metrictank")
)

func apiIngestInit() {
	if !*ingestEnabled {
		return
	}
	log.Info("api input enabled")
	if *schemaFile == "" {
		log.Fatal(4, "no schema file configured for api ingest")
	}
	s, err := conf.ReadSchemas(*schemaFile)
	if err != nil {
		log.Fatal(4, "failed to load carbon schemas config. %s", err)
	}
	schemas = &s
}
