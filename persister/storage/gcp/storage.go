package gcp

import (
	"context"
	"encoding/json"
	"flag"
	"strconv"

	"cloud.google.com/go/bigtable"
	"github.com/raintank/tsdb-gw/ingest/datadog"
	"github.com/raintank/tsdb-gw/persister/storage"
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

// storageClient implements storage.Storage for GCP.
type storageClient struct {
	cfg         Config
	client      *bigtable.Client
	adminClient *bigtable.AdminClient
	tablename   string
	prefix      string
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

	return &storageClient{
		cfg:         cfg,
		client:      client,
		adminClient: adminClient,
		tablename:   cfg.TableName,
		prefix:      cfg.Prefix,
	}, nil
}

func (s *storageClient) Store(data datadog.DataDogIntakePayload) error {
	table := s.client.Open(s.tablename)
	mut := bigtable.NewMutation()
	buf, err := json.Marshal(data)
	if err != nil {
		return err
	}

	mut.Set("ddIntake", "metricdata", bigtable.Now(), buf)
	rowKey := strconv.Itoa(data.OrgID) + ":" + data.InternalHostname

	err = table.Apply(context.Background(), rowKey, mut)
	if err != nil {
		return err
	}

	return nil
}

func (s *storageClient) Retrieve() ([]datadog.DataDogIntakePayload, error) {
	tbl := s.client.Open(s.tablename)
	rr := bigtable.PrefixRange(s.prefix)
	var payloads []datadog.DataDogIntakePayload
	err := tbl.ReadRows(context.Background(), rr, func(r bigtable.Row) bool {
		log.Debugf("loading from row %v", r.Key())
		data, ok := r["ddIntake"]
		if !ok {
			return true
		}
		d := datadog.DataDogIntakePayload{}
		err := json.Unmarshal(data[0].Value, &d)
		if err != nil {
			log.Errorf("unable to decode metric from row %v; Reason: %v", r.Key(), err)
			return false
		}
		payloads = append(payloads, d)
		return true
	}, bigtable.RowFilter(bigtable.FamilyFilter("ddIntake")))

	if err != nil {
		return nil, err
	}

	return payloads, nil
}

func (s *storageClient) Remove(orgID int, host string) error {
	err := s.adminClient.DropRowRange(context.Background(), s.tablename, strconv.Itoa(orgID)+":"+host)
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
