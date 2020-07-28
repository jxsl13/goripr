package main

import (
	"log"
	"math/rand"
	"testing"
	"time"
)

type rangeReason struct {
	Range  string
	Reason string
}

var (
	ranges = [...]rangeReason{
		{"200.0.0.0 - 230.0.0.0", "first"},
		{"210.0.0.0 - 220.0.0.0", "second"},
		{"190.0.0.0 - 205.0.0.0", "third"},
		{"205.0.0.0 - 225.0.0.0", "fourth"},
		{"201.0.0.0 - 202.0.0.0", "fifth"},
		{"203.0.0.0 - 204.0.0.0", "seventh"},
		{"205.0.0.0 - 235.0.0.0", "eighth"},
		{"190.0.0.0 - 235.0.0.0", "ninth"},
		{"190.0.0.0 - 195.0.0.0", "10th"},
		{"195.0.0.0 - 196.0.0.0", "11th"},
		{"196.0.0.0 - 197.0.0.0", "12th"},
		{"197.0.0.0 - 235.0.0.0", "13th"},
		{"188.0.0.0 - 198.0.0.0", "14th"},
		{"188.0.0.0 - 235.0.0.0", "15th"},
		{"188.0.0.0 - 235.0.0.255", "16th"},
		{"187.255.255.255 - 235.0.1.0", "17th"},
		{"188.0.0.1 - 235.0.0.254", "18th"},
		{"123.0.0.0 - 123.0.0.10", "19th"},
		{"123.0.0.1 - 123.0.0.9", "20th"},
		{"235.0.0.255", "21st"},
		{"188.0.0.0", "22nd"},
		{"188.0.0.0", "23rd"},
		{"123.0.0.0 - 123.0.0.2", "24th"},
		{"123.0.0.1", "25th"},
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
	for idx, attr := range attributes {
		t.Logf("\tidx=%4d\t%16s\tupper: %5t\tlower: %5t\t%20s", idx, attr.IP.String(), attr.UpperBound, attr.LowerBound, attr.Reason)
	}

	cnt := 0
	state := 0
	for idx, attr := range attributes {

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

			// reasons consistent
			if idx > 0 && attr.Reason != attributes[idx-1].Reason {
				t.Errorf("reason mismatch: idx=%4d reason=%q idx=%4d reason=%q", idx-1, attributes[idx-1].Reason, idx, attr.Reason)
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

func shuffle(a []rangeReason) []rangeReason {
	var b []rangeReason
	copy(b, a)

	seed := time.Now().UnixNano()
	log.Println("seed =", seed)
	rand.Seed(seed)
	rand.Shuffle(len(b), func(i, j int) { b[i], b[j] = b[j], b[i] })
	return b
}

func TestRedisClient_Insert(t *testing.T) {

	rdb := initRDB()
	defer rdb.Close()

	type args struct {
		ipRanges []rangeReason
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"simple insert all", args{ranges[:]}, false},
		{"shuffle 1", args{shuffle(ranges[:])}, false},
		{"shuffle 2", args{shuffle(ranges[:])}, false},
		{"shuffle 3", args{shuffle(ranges[:])}, false},
		{"shuffle 4", args{shuffle(ranges[:])}, false},
		{"shuffle 5", args{shuffle(ranges[:])}, false},
		{"shuffle 7", args{shuffle(ranges[:])}, false},
		{"shuffle 8", args{shuffle(ranges[:])}, false},
		{"shuffle 9", args{shuffle(ranges[:])}, false},
		{"shuffle 10", args{shuffle(ranges[:])}, false},
	}
	for _, tt := range tests {

		t.Run(tt.name, func(t *testing.T) {

			// consistency after every insert
			for _, ipRange := range tt.args.ipRanges {

				if err := rdb.Insert(ipRange.Range, ipRange.Reason); (err != nil) != tt.wantErr {
					t.Errorf("rdb.Insert() error = %v, wantErr %v", err, tt.wantErr)
				}

				if !consistent(rdb, t) {
					t.Fatalf("rdb.Insert() error : Database INCONSISTENT after inserting range: %s", ipRange.Range)
				} else {
					t.Logf("rdb.Insert() Info  : Database is CONSISTENT after inserting range: %s", ipRange.Range)
				}
			}
			rdb.FlushAll().Result()
		})
	}
}
