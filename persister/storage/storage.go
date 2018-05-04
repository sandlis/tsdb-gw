package storage

import "github.com/raintank/tsdb-gw/ingest/datadog"

type Storage interface {
	Store(data datadog.DataDogIntakePayload) error
	Retrieve() ([]datadog.DataDogIntakePayload, error)
	Remove(orgID int, hostname string) error
}
