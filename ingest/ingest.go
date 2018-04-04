package ingest

import (
	"flag"

	"github.com/grafana/metrictank/conf"
	"github.com/raintank/tsdb-gw/util"
	log "github.com/sirupsen/logrus"
)

var (
	metricPool    = util.NewMetricDataPool()
	schemas       *conf.Schemas
	schemaFile    = flag.String("api-schemas-file", "/etc/storage-schemas.conf", "path to carbon storage-schemas.conf file")
	ingestEnabled = flag.Bool("api-ingest", true, "enable api ingest for datadog/opentsdb/prometheus for metrictank")
)

func IngestInit() {
	if !*ingestEnabled {
		return
	}
	log.Info("api ingest enabled")
	if *schemaFile == "" {
		log.Fatalln("no schema file configured for api ingest")
	}
	s, err := conf.ReadSchemas(*schemaFile)
	if err != nil {
		log.Fatalf("failed to load carbon schemas config. %s", err)
	}
	schemas = &s
}
