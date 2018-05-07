package storage

import (
	"context"
	"encoding/json"
	"flag"

	"cloud.google.com/go/bigtable"
	"github.com/raintank/tsdb-gw/ingest/datadog"
	log "github.com/sirupsen/logrus"
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

// Client interfaces with bigtable to store persisted metrics payloads
type Client struct {
	cfg       Config
	client    *bigtable.Client
	tablename string
	prefix    string
}

// New returns a new Client.
func New(ctx context.Context, cfg Config) (*Client, error) {
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

	if !sliceContains(tblInfo.Families, "ddIntake") {
		if err := adminClient.CreateColumnFamily(context.Background(), cfg.TableName, "ddIntake"); err != nil {
			log.Errorf("Could not create column family %v", "ddIntake")
			return nil, err
		}
	}

	client, err := bigtable.NewClient(ctx, cfg.Project, cfg.Instance)
	if err != nil {
		return nil, err
	}

	return &Client{
		cfg:       cfg,
		client:    client,
		tablename: cfg.TableName,
		prefix:    cfg.Prefix,
	}, nil
}

// Store persists a payload to bigtable
func (s *Client) Store(rowKey string, data datadog.PersistPayload) error {
	table := s.client.Open(s.tablename)
	mut := bigtable.NewMutation()
	buf, err := json.Marshal(data)
	if err != nil {
		return err
	}

	mut.Set("ddIntake", "metricdata", bigtable.Now(), buf)

	err = table.Apply(context.Background(), rowKey, mut)
	if err != nil {
		return err
	}

	return nil
}

// Retrieve unmarshals and returns metrics payloads from bigtable
func (s *Client) Retrieve() (map[string]datadog.DataDogIntakePayload, error) {
	tbl := s.client.Open(s.tablename)
	rr := bigtable.PrefixRange(s.prefix)
	intakes := map[string]datadog.DataDogIntakePayload{}
	err := tbl.ReadRows(context.Background(), rr, func(r bigtable.Row) bool {
		log.Debugf("loading from row %v", r.Key())
		data, ok := r["ddIntake"]
		if !ok {
			return true
		}
		payload := datadog.PersistPayload{}
		err := json.Unmarshal(data[0].Value, &payload)
		if err != nil {
			log.Errorf("unable to decode metric from row %v; Reason: %v", r.Key(), err)
			return false
		}

		intake := datadog.DataDogIntakePayload{}
		err = json.Unmarshal(payload.Raw, &intake)
		if err != nil {
			log.Errorf("unable to decode metric from row %v; Reason: %v", r.Key(), err)
			return false
		}
		intake.OrgID = payload.OrgID
		intakes[r.Key()] = intake
		return true
	}, bigtable.RowFilter(bigtable.FamilyFilter("ddIntake")))

	if err != nil {
		return nil, err
	}

	return intakes, nil
}

// Remove removes a set of rowkeys from bigtable
func (s *Client) Remove(rowKeys []string) error {
	tbl := s.client.Open(s.tablename)
	muts := make([]*bigtable.Mutation, len(rowKeys))
	for i := range rowKeys {
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		muts[i] = mut
	}

	errs, err := tbl.ApplyBulk(context.Background(), rowKeys, muts)
	if len(errs) > 0 {
		for _, e := range errs {
			log.Error(e)
		}
	}

	if err != nil {
		return err
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
