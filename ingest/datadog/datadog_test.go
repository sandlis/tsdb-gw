package datadog

import (
	"reflect"
	"testing"
)

func Test_createTagSet(t *testing.T) {
	type args struct {
		host   string
		device string
		ctags  []string
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{
			name: "basic metric",
			args: args{
				host:   "localhost",
				device: "",
				ctags:  []string{"example:tag"},
			},
			want: []string{"example=tag", "host=localhost"},
		},
		{
			name: "device metric",
			args: args{
				host:   "localhost",
				device: "sda0",
				ctags:  []string{"example:tag"},
			},
			want: []string{"device=sda0", "example=tag", "host=localhost"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := createTagSet(tt.args.host, tt.args.device, tt.args.ctags); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("createTagSet() = %v, want %v", got, tt.want)
			}
		})
	}
}
