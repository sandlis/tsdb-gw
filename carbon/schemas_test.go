package carbon

import (
	"testing"

	"github.com/raintank/metrictank/conf"
)

var (
	schemas = conf.NewSchemas([]conf.Schema{})
)

func Test_parseMetric(t *testing.T) {
	type args struct {
		buf     []byte
		schemas *conf.Schemas
		orgID   int
	}
	tests := []struct {
		name     string
		args     args
		wantName string
		wantTags []string
		wantErr  bool
	}{
		{
			name: "simple metric",
			args: args{
				buf:     []byte("test.metric.value 10 10"),
				schemas: &schemas,
				orgID:   1,
			},
			wantName: "test.metric.value",
			wantErr:  false,
		},
		{
			name: "simple metric with tags",
			args: args{
				buf:     []byte("test.metric.value;test=tag 10 10"),
				schemas: &schemas,
				orgID:   1,
			},
			wantName: "test.metric.value",
			wantTags: []string{"test=tag"},
			wantErr:  false,
		},
		{
			name: "simple metric with two tags",
			args: args{
				buf:     []byte("test.metric.value;tag1=test;tag2=test 10 10"),
				schemas: &schemas,
				orgID:   1,
			},
			wantName: "test.metric.value",
			wantTags: []string{"tag1=test", "tag2=test"},
			wantErr:  false,
		},
		{
			name: "simple metric with two unsorted tags",
			args: args{
				buf:     []byte("test.metric.value;tag2=test;tag1=test 10 10"),
				schemas: &schemas,
				orgID:   1,
			},
			wantName: "test.metric.value",
			wantTags: []string{"tag1=test", "tag2=test"},
			wantErr:  false,
		},
		{
			name: "extra semi-colon metric",
			args: args{
				buf:     []byte("test.metric.value; 10 10"),
				schemas: &schemas,
				orgID:   1,
			},
			wantErr: true,
		},
		{
			name: "bad tag metric",
			args: args{
				buf:     []byte("test.metric.value;= 10 10"),
				schemas: &schemas,
				orgID:   1,
			},
			wantErr: true,
		},
		{
			name: "no key tag metric",
			args: args{
				buf:     []byte("test.metric.value;=test 10 10"),
				schemas: &schemas,
				orgID:   1,
			},
			wantErr: true,
		},
		{
			name: "no value tag metric",
			args: args{
				buf:     []byte("test.metric.value;tag1= 10 10"),
				schemas: &schemas,
				orgID:   1,
			},
			wantName: "test.metric.value",
			wantTags: []string{"tag1="},
			wantErr:  false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseMetric(tt.args.buf, tt.args.schemas, tt.args.orgID)
			if (err != nil) != tt.wantErr {
				t.Errorf("parseMetric() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr {
				return
			}
			if tt.wantName != got.Name {
				t.Errorf("name %v, want %v", got.Name, tt.wantName)
			}
			if len(tt.wantTags) != len(got.Tags) {
				t.Errorf("tags '%v', want'%v'", got.Tags, tt.wantTags)
			}
			for i := range tt.wantTags {
				if tt.wantTags[i] != got.Tags[i] {
					t.Errorf("exptected tag to match, tag=%v, want %v", got.Tags[i], tt.wantTags[i])
				}
			}
		})
	}
}
