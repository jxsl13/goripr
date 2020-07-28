package main

import (
	"fmt"
	"math/big"
	"math/rand"
	"testing"
	"time"
)

type rangeReason struct {
	Range  string
	Reason string
}

var (
	ranges = []rangeReason{
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
		{"123.0.0.2", "26th"},
		{"123.0.0.3", "27th"},
		{"123.0.0.4", "28th"},
		{"123.0.0.5", "29th"},
		{"123.0.0.6", "30th"},
		{"123.0.0.7", "31st"},
		{"123.0.0.8", "32nd"},
		{"123.0.0.1 - 123.0.0.2", "33rd"},
		{"123.0.0.1 - 123.0.0.3", "34th"},
		{"123.0.0.1 - 123.0.0.4", "35th"},
		{"123.0.0.1 - 123.0.0.5", "36th"},
		{"123.0.0.1 - 123.0.0.6", "37th"},
		{"123.0.0.1 - 123.0.0.7", "38th"},
		{"123.0.0.1 - 123.0.0.8", "39th"},
		{"123.0.0.1 - 123.0.0.9", "40th"},
		{"123.0.0.1 - 123.0.0.10", "41st"},
		{"123.0.0.2 - 123.0.0.10", "42nd"},
		{"123.0.0.3 - 123.0.0.10", "43rd"},
		{"123.0.0.4 - 123.0.0.10", "44th"},
		{"123.0.0.5 - 123.0.0.10", "45th"},
	}
)

type args struct {
	ipRanges []rangeReason
}

type testCase struct {
	name    string
	args    args
	wantErr bool
}

// Tests whether the database is in a cosistent state.
func consistent(rdb *RedisClient, t *testing.T) bool {
	attributes, err := rdb.insideInfRange()
	if err != nil {
		panic(err)
	}

	const LowerBound = 0
	const UpperBound = 1

	t.Logf("%d attributes fetched from database.", len(attributes))
	for idx, attr := range attributes {
		t.Logf("\tidx=%4d\t%16s\tlower: %5t\tupper: %5t\t%20s", idx, attr.IP.String(), attr.LowerBound, attr.UpperBound, attr.Reason)
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

// generateRange generates a valid IP range
// and and returns a random IP that is within the range
func generateRange() (ipRange string, insideIP string) {

	rand.Seed(time.Now().UnixNano())
	low := int64(rand.Int31())
	rand.Seed(time.Now().UnixNano())
	high := int64(rand.Int31())

	if low > high {
		low, high = high, low
	}

	rand.Seed(time.Now().UnixNano())

	between := low
	if high-low > 0 {
		between = rand.Int63n(high - low)
	}

	lowIP := IntToIP(big.NewInt(low), IPv4Bits).String()
	highIP := IntToIP(big.NewInt(high), IPv4Bits).String()

	betweenIP := IntToIP(big.NewInt(between), IPv4Bits).String()

	hyphenRange := fmt.Sprintf("%s - %s", lowIP, highIP)

	rand.Seed(time.Now().UnixNano())
	mask := rand.Intn(32)

	cidrRange := fmt.Sprintf("%s/%d", lowIP, mask)

	rand.Seed(time.Now().UnixNano())

	if rand.Int()%2 == 0 {
		return hyphenRange, betweenIP
	}

	return cidrRange, betweenIP

}

func initRDB(db int) *RedisClient {
	if db > 15 {
		panic("redis only supports database indices from 0 through 15.")
	}
	rdb, err := NewRedisClient(RedisOptions{
		Addr:     "localhost:6379",
		Password: "",
		DB:       db,
	})
	if err != nil {
		panic(err)
	}

	_, err = rdb.FlushDB().Result()
	if err != nil {
		panic(err)
	}

	rdb.Close()

	rdb, err = NewRedisClient(RedisOptions{
		Addr:     "localhost:6379",
		Password: "",
		DB:       db,
	})
	if err != nil {
		panic(err)
	}
	return rdb
}

func shuffle(seed int64, a []rangeReason) []rangeReason {
	var b []rangeReason
	copy(b, a)
	rand.Seed(seed)
	rand.Shuffle(len(b), func(i, j int) { b[i], b[j] = b[j], b[i] })
	return b
}

func initRanges() {
	// generate ranges
	for i := 0; i < 1000; i++ {
		ipRange, _ := generateRange()
		ranges = append(ranges, rangeReason{
			Range:  ipRange,
			Reason: fmt.Sprintf("random %5d", i),
		})
	}
}

func TestRedisClient_Insert(t *testing.T) {
	// generate random ranges
	initRanges()

	// initial test
	tests := []testCase{
		{"simple insert all", args{ranges[:]}, false},
	}

	// shuffle initial test to generate new tests
	for i := 0; i < 1000; i++ {
		seed := time.Now().UnixNano()
		rand.Seed(seed)
		tests = append(tests, testCase{
			fmt.Sprintf("shuffle %d, seed=%q", i, seed),
			args{shuffle(seed, ranges[:])},
			false,
		})
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdb := initRDB(0)
			defer rdb.Close()

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
			rdb.FlushDB().Result()
		})
	}
}
