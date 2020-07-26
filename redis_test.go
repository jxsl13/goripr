package main

import (
	"testing"
)

var (
	ranges = [...]string{
		"200.0.0.0 - 230.0.0.0",
		"210.0.0.0 - 220.0.0.0",
		"190.0.0.0 - 205.0.0.0",
		"205.0.0.0 - 225.0.0.0",
		"201.0.0.0 - 202.0.0.0",
		"203.0.0.0 - 204.0.0.0",
		"205.0.0.0 - 235.0.0.0",
	}
)

func consistent(rdb *RedisClient) bool {
	attributes, err := rdb.Inside("0.0.0.0 - 255.255.255.255")
	if err != nil {
		panic(err)
	}
	const LowerBound = 1
	const UpperBound = 0

	cnt := 0
	state := 0
	for idx, attr := range attributes {
		if idx == 0 && !attr.LowerBound {
			return false
		}

		if attr.LowerBound && attr.UpperBound {
			if state != UpperBound {
				return false
			}

			cnt += 2
		} else if attr.LowerBound {
			if state != UpperBound {
				return false
			}
			cnt++
			state = cnt % 2
		} else if attr.UpperBound {
			if state != LowerBound {
				return false
			}
			cnt++
			state = cnt % 2
		}
	}

	return state == UpperBound
}

func initRDB() *RedisClient {
	rdb, err := NewRedisClient(RedisOptions{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	if err != nil {
		panic(err)
	}
	return rdb
}

func TestRedisClient_Insert(t *testing.T) {

	type args struct {
		ipRanges []string
		reason   string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"simple insert all", args{ranges[:], "simple insert all"}, false},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {
			rdb := initRDB()
			defer rdb.Close()

			// initial consistency
			if !consistent(rdb) {
				t.Errorf("RedisClient.Insert() error = Database Inconsistent before test")
			}

			// consistency after every insert
			for _, ipRange := range tt.args.ipRanges {
				if err := rdb.Insert(ipRange, tt.args.reason); (err != nil) != tt.wantErr {
					t.Errorf("RedisClient.Insert() error = %v, wantErr %v", err, tt.wantErr)
				}

				if !consistent(rdb) {
					t.Errorf("RedisClient.Insert() error = Database Inconsistent after inserting range: %s", ipRange)
				}
			}

			rdb.FlushAll().Result()
		})
	}
}
