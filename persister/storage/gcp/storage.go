package gcp

import (
	"context"
	"flag"
	"fmt"

	"cloud.google.com/go/bigtable"
	"github.com/raintank/tsdb-gw/persister/storage"
	log "github.com/sirupsen/logrus"
	schema "gopkg.in/raintank/schema.v1"
)

// Config for a StorageClient
type Config struct {
	Project   string
	Instance  string
	TableName string
	Prefix    string
}

// RegisterFlags adds the flags required to config this to the given FlagSet
func (cfg *Config) RegisterFlags(f *flag.FlagSet) {
	f.StringVar(&cfg.Project, "bigtable-project", "persister", "Bigtable project ID.")
	f.StringVar(&cfg.Instance, "bigtable-instance", "persister", "Bigtable instance ID.")
	f.StringVar(&cfg.TableName, "bigtable-tablename", "persister", "Bigtable table name f.")
	f.StringVar(&cfg.Prefix, "bigtable-prefix", "", "row prefix to use when loading metrics")
}

// storageClient implements storage.Storage for GCP.
type storageClient struct {
	cfg       Config
	client    *bigtable.Client
	tablename string
	prefix    string
}

// NewStorageClient returns a new StorageClient.
func NewStorageClient(ctx context.Context, cfg Config) (storage.Storage, error) {
	adminClient, err := bigtable.NewAdminClient(ctx, cfg.Project, cfg.Instance)
	if err != nil {
		return nil, err
	}

	tables, err := adminClient.Tables(context.Background())
	if err != nil {
		log.Errorf("Could not fetch table list: %v", err)
		return nil, err
	}

	if !sliceContains(tables, cfg.TableName) {
		log.Printf("Creating table %s", cfg.TableName)
		if err := adminClient.CreateTable(context.Background(), cfg.TableName); err != nil {
			log.Errorf("Could not create table %s", cfg.TableName)
			return nil, err
		}
	}

	tblInfo, err := adminClient.TableInfo(context.Background(), cfg.TableName)
	if err != nil {
		log.Errorf("Could not read info for table %s", cfg.TableName)
		return nil, err
	}

	if !sliceContains(tblInfo.Families, "metrics") {
		if err := adminClient.CreateColumnFamily(context.Background(), cfg.TableName, "metrics"); err != nil {
			log.Errorf("Could not create column family %v", "metrics")
			return nil, err
		}
	}

	client, err := bigtable.NewClient(ctx, cfg.Project, cfg.Instance)
	if err != nil {
		return nil, err
	}

	return &storageClient{
		cfg:       cfg,
		client:    client,
		tablename: cfg.TableName,
		prefix:    cfg.Prefix,
	}, nil
}

func (s *storageClient) Store(metrics []*schema.MetricData) error {
	if len(metrics) < 1 {
		return nil
	}
	table := s.client.Open(s.tablename)
	var data []byte
	rowKeys := make([]string, 0, len(metrics))
	muts := make([]*bigtable.Mutation, 0, len(metrics))
	for _, m := range metrics {
		m.SetId()
		msg, err := m.MarshalMsg(data)
		if err != nil {
			log.Errorf("unable to marshal metric: %v", m.Id)
			return err
		}
		mut := bigtable.NewMutation()
		mut.Set("metrics", "metricdata", bigtable.Now(), msg)
		muts = append(muts, mut)
		rowKeys = append(rowKeys, m.Id)
	}

	errs, err := table.ApplyBulk(context.Background(), rowKeys, muts)
	if err != nil {
		return err
	}
	if len(errs) > 0 {
		for _, e := range errs {
			log.Error(e)
		}
	}

	return nil
}

func (s *storageClient) Retrieve() ([]*schema.MetricData, error) {
	tbl := s.client.Open(s.tablename)
	rr := bigtable.PrefixRange(s.prefix)
	var metrics []*schema.MetricData
	err := tbl.ReadRows(context.Background(), rr, func(r bigtable.Row) bool {
		m := &schema.MetricData{}
		_, err := m.UnmarshalMsg(r["metrics"][0].Value)
		if err != nil {
			log.Errorf("unable to decode metric from row %v", r.Key())
			return false
		}
		metrics = append(metrics, m)
		return true
	}, bigtable.RowFilter(bigtable.FamilyFilter("metrics")))

	if err != nil {
		return nil, err
	}

	return metrics, nil
}

func (s *storageClient) Remove(metrics []*schema.MetricData) error {
	if len(metrics) < 1 {
		return fmt.Errorf("empty metrics slice")
	}

	tbl := s.client.Open(s.tablename)
	muts := make([]*bigtable.Mutation, len(metrics))
	rowKeys := make([]string, len(metrics))
	for i, m := range metrics {
		m.SetId()
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		muts[i] = mut
		rowKeys[i] = m.Id
		log.Debugf("removing metric %v", m)
	}

	errs, err := tbl.ApplyBulk(context.Background(), rowKeys, muts)
	if err != nil {
		return err
	}
	if len(errs) > 0 {
		for _, e := range errs {
			log.Errorln(e)
		}
	}

	return nil
}

func sliceContains(list []string, target string) bool {
	for _, s := range list {
		if s == target {
			return true
		}
	}
	return false
}
