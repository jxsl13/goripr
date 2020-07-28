package goripr

import (
	"net"
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

func TestAddressRange(t *testing.T) {
	// reference: https://mxtoolbox.com/subnetcalculator.aspx
	type args struct {
		network string
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 string
	}{
		{"/1", args{"1.163.180.155/1"}, "0.0.0.0", "127.255.255.255"},
		{"/2", args{"1.163.180.155/2"}, "0.0.0.0", "63.255.255.255"},
		{"/3", args{"1.163.180.155/3"}, "0.0.0.0", "31.255.255.255"},
		{"/4", args{"1.163.180.155/4"}, "0.0.0.0", "15.255.255.255"},
		{"/5", args{"1.163.180.155/5"}, "0.0.0.0", "7.255.255.255"},
		{"/6", args{"1.163.180.155/6"}, "0.0.0.0", "3.255.255.255"},
		{"/7", args{"1.163.180.155/7"}, "0.0.0.0", "1.255.255.255"},
		{"/8", args{"1.163.180.155/8"}, "1.0.0.0", "1.255.255.255"},
		{"/9", args{"1.163.180.155/9"}, "1.128.0.0", "1.255.255.255"},
		{"/10", args{"1.163.180.155/10"}, "1.128.0.0", "1.191.255.255"},
		{"/11", args{"1.163.180.155/11"}, "1.160.0.0", "1.191.255.255"},
		{"/12", args{"1.163.180.155/12"}, "1.160.0.0", "1.175.255.255"},
		{"/13", args{"1.163.180.155/13"}, "1.160.0.0", "1.167.255.255"},
		{"/14", args{"1.163.180.155/14"}, "1.160.0.0", "1.163.255.255"},
		{"/15", args{"1.163.180.155/15"}, "1.162.0.0", "1.163.255.255"},
		{"/16", args{"1.163.180.155/16"}, "1.163.0.0", "1.163.255.255"},
		{"/17", args{"1.163.180.155/17"}, "1.163.128.0", "1.163.255.255"},
		{"/18", args{"1.163.180.155/18"}, "1.163.128.0", "1.163.191.255"},
		{"/19", args{"1.163.180.155/19"}, "1.163.160.0", "1.163.191.255"},
		{"/20", args{"1.163.180.155/20"}, "1.163.176.0", "1.163.191.255"},
		{"/21", args{"1.163.180.155/21"}, "1.163.176.0", "1.163.183.255"},
		{"/22", args{"1.163.180.155/22"}, "1.163.180.0", "1.163.183.255"},
		{"/23", args{"1.163.180.155/23"}, "1.163.180.0", "1.163.181.255"},
		{"/24", args{"1.163.180.155/24"}, "1.163.180.0", "1.163.180.255"},
		{"/25", args{"1.163.180.155/25"}, "1.163.180.128", "1.163.180.255"},
		{"/26", args{"1.163.180.155/26"}, "1.163.180.128", "1.163.180.191"},
		{"/27", args{"1.163.180.155/27"}, "1.163.180.128", "1.163.180.159"},
		{"/28", args{"1.163.180.155/28"}, "1.163.180.144", "1.163.180.159"},
		{"/29", args{"1.163.180.155/29"}, "1.163.180.152", "1.163.180.159"},
		{"/30", args{"1.163.180.155/30"}, "1.163.180.152", "1.163.180.155"},
		{"/31", args{"1.163.180.155/31"}, "1.163.180.154", "1.163.180.155"},
		{"/32", args{"1.163.180.155/32"}, "1.163.180.155", "1.163.180.155"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ipnet, err := net.ParseCIDR(tt.args.network)
			if err != nil {
				panic(err)
			}
			got, got1 := AddressRange(ipnet)
			wantIP := net.ParseIP(tt.want).To4()
			if !reflect.DeepEqual(got, wantIP) {
				t.Errorf("AddressRange() got = %v, want %v", got, tt.want)
			}
			wantIP1 := net.ParseIP(tt.want1).To4()
			if !reflect.DeepEqual(got1, wantIP1) {
				t.Errorf("AddressRange() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
