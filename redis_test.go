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

func consistent(rdb *RedisClient, t *testing.T) bool {
	attributes, err := rdb.insideInfRange()
	if err != nil {
		panic(err)
	}
	const LowerBound = 0
	const UpperBound = 1

	t.Logf("%d attributes fetched from database.", len(attributes))
	for _, attr := range attributes {
		t.Logf("\t\t%16s\tupper: %5t\tlower: %5t", attr.IP.String(), attr.UpperBound, attr.LowerBound)
	}

	cnt := 0
	state := 0
	for _, attr := range attributes {

		if attr.LowerBound && attr.UpperBound {
			if state != LowerBound {
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

	return state == LowerBound
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

	_, err = rdb.FlushAll().Result()
	if err != nil {
		panic(err)
	}

	rdb.Close()

	rdb, err = NewRedisClient(RedisOptions{
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

	rdb := initRDB()
	defer rdb.Close()

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

			// consistency after every insert
			for _, ipRange := range tt.args.ipRanges {

				if err := rdb.Insert(ipRange, tt.args.reason); (err != nil) != tt.wantErr {
					t.Errorf("RedisClient.Insert() error = %v, wantErr %v", err, tt.wantErr)
				}

				if !consistent(rdb, t) {
					t.Fatalf("RedisClient.Insert() error : Database inconsistent after inserting range: %s", ipRange)
				} else {
					t.Logf("RedisClient.Insert() Info  : Database is consistent after inserting range: %s", ipRange)
				}
			}

			rdb.FlushAll().Result()
		})
	}
}
