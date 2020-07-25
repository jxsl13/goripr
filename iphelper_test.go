package main

import (
	"reflect"
	"testing"
)

func TestBoundaries(t *testing.T) {
	type args struct {
		ipRange string
	}
	tests := []struct {
		name     string
		args     args
		wantLow  string
		wantHigh string
		wantErr  bool
	}{
		{"range normal inverted", args{"123.0.0.255 - 123.0.0.0"}, "<nil>", "<nil>", true},
		{"cidr", args{"123.0.0.1/24"}, "123.0.0.0", "123.0.0.255", false},
		{"cidr with comment", args{"123.0.0.1/24#comment"}, "123.0.0.0", "123.0.0.255", false},
		{"range normal", args{"123.0.0.0 - 123.0.0.255"}, "123.0.0.0", "123.0.0.255", false},
		{"range normal comment", args{"123.0.0.0 - 123.0.0.255 # comment"}, "123.0.0.0", "123.0.0.255", false},
		{"range no space", args{"123.0.0.0-123.0.0.255"}, "123.0.0.0", "123.0.0.255", false},
		{"range no space comment", args{"123.0.0.0-123.0.0.255#comment"}, "123.0.0.0", "123.0.0.255", false},
		{"ipv6 cidr", args{"fe80::204:61ff:fe9d:f156/120"}, "fe80::204:61ff:fe9d:f100", "fe80::204:61ff:fe9d:f1ff", false},
		{"ipv6 cidr", args{"fe80::204:61ff:fe9d:f156/120"}, "fe80::204:61ff:fe9d:f100", "fe80::204:61ff:fe9d:f1ff", false},
		{"error no ip", args{"comment"}, "<nil>", "<nil>", true},
		{"error malformed ip", args{"a.123.0.0"}, "<nil>", "<nil>", true},
		{"error malformed ipv4 cidr", args{"a.123.0.0/24"}, "<nil>", "<nil>", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLow, gotHigh, err := Boundaries(tt.args.ipRange)
			if (err != nil) != tt.wantErr {
				t.Errorf("Boundaries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotLow.String(), tt.wantLow) {
				t.Errorf("Boundaries() gotLow = %v, want %v", gotLow, tt.wantLow)
			}
			if !reflect.DeepEqual(gotHigh.String(), tt.wantHigh) {
				t.Errorf("Boundaries() gotHigh = %v, want %v", gotHigh, tt.wantHigh)
			}
		})
	}
}
