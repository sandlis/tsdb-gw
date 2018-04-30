package storage

import schema "gopkg.in/raintank/schema.v1"

type Storage interface {
	Store([]*schema.MetricData) error
	Retrieve(orgID int) ([]*schema.MetricData, error)
}
