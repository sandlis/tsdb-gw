package kafka

import (
	"testing"

	p "github.com/grafana/metrictank/cluster/partitioner"
	"gopkg.in/raintank/schema.v1"
)

func TestPartitioning(t *testing.T) {
	testData := []schema.MetricData{
		{Name: "name1"},
		{Name: "name2"},
		{Name: "name3"},
		{Name: "name4"},
		{Name: "name5"},
		{Name: "name6"},
		{Name: "name7"},
		{Name: "name8"},
		{Name: "name9"},
		{Name: "name10"},
	}

	kafkaPartitioner, _ = p.NewKafka("bySeries")

	var data []byte
	part_new := NewPartitioner()

	for partitionCount = 1; partitionCount <= 64; partitionCount++ {
		for _, m := range testData {
			data, _ = m.MarshalMsg(data)
			res_old, _ := kafkaPartitioner.Partition(&m, partitionCount)
			key_old, _ := kafkaPartitioner.GetPartitionKey(&m, nil)
			res_new, key_new, _ := part_new.partition(&m)
			if res_old != res_new {
				t.Fatalf("results did not match with %d partitionCount for %+v", partitionCount, m)
			}

			if string(key_old) != string(key_new) {
				t.Fatalf("keys did not match with %d partitionCount for %+v", partitionCount, m)
			}
		}
	}
}
