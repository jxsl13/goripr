package goripr

import (
	"fmt"
	"math"
	"math/rand"
	"regexp"
	"testing"
	"time"

	"github.com/xgfone/netaddr"
	//"runtime"
	//"strings"
)

type rangeReason struct {
	Range  string
	Reason string
}

var (
	ranges = []rangeReason{
		{"120.2.2.2/1", "zero"},
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
		{"98.231.84.169 - 114.253.39.105", "46th"},
		{"122.29.207.117 - 122.29.207.117", "47th"},
		{"36.194.221.128 - 118.245.65.201", "48th"},
		{"86.196.27.130 - 101.181.15.63", "49th"},
		{"101.181.15.64 - 101.181.15.95", "50th"},
		{"101.181.15.96 - 123.10.177.145", "51st"},
		{"123.10.177.146 - 127.134.179.196", "52nd"},
		{"19.188.174.203 - 101.181.207.70", "53rd"},
		// {"", "53rd"},
		// {"", "54th"},
		// {"", "55th"},

	}
)

func TestClient_Insert(t *testing.T) {
	// generate random ranges
	initRanges(100)

	// initial test
	tests := []testCase{
		{"cut below and cut above hit a boundary",
			[]rangeReason{
				{"123.0.0.0 - 123.0.0.2", "1st"},
				{"123.0.0.4 - 123.0.0.6", "2nd"},
				{"123.0.0.3", "3rd"},
				{"123.0.0.1 - 123.0.0.5", "4th"},
			},
			false,
		},
		{"simple insert all", ranges, false},
	}

	// shuffle initial test to generate new tests
	for i := 0; i < 10; i++ {
		seed := time.Now().UnixNano()

		shuffledRange := make([]rangeReason, len(ranges))
		copy(shuffledRange, ranges)
		shuffle(seed, shuffledRange)

		tests = append(tests, testCase{
			fmt.Sprintf("shuffle %d, seed=%d", i, seed),
			shuffledRange,
			false,
		})
	}

	for idx, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rdb := initRDB(0)
			defer rdb.Close()

			// consistency after every insert
			for _, ipRange := range tt.ipRanges {

				if err := rdb.Insert(ipRange.Range, ipRange.Reason); (err != nil) != tt.wantErr {
					t.Errorf("rdb.Insert() error = %v, wantErr %v, range passed: %q", err, tt.wantErr, ipRange.Range)
					return
				}

				if !consistent(rdb, t, ipRange.Range, idx) {
					t.Errorf("rdb.Insert() error : Database INCONSISTENT after inserting range: %s", ipRange.Range)
					return
				}
				t.Logf("rdb.Insert() Info  : Database is CONSISTENT after inserting range: %s", ipRange.Range)

			}
		})
	}
}

func TestClient_Find(t *testing.T) {

	tests := initTestCasesFind(100)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			rdb := initRDB(0)
			defer rdb.Close()

			for idx, rir := range tt.ipRanges {
				ipToFind := rir.IP
				reasonToFind := rir.Reason
				rangeToFind := rir.Range

				err := rdb.Insert(rangeToFind, reasonToFind)
				if err != nil {
					t.Errorf("rdb.Insert() error = %v, wantErr %v", err, tt.wantErr)
					return
				}

				if !consistent(rdb, t, rangeToFind, idx) {
					t.Fatalf("database inconsistent")
				}

				got, err := rdb.Find(ipToFind)

				if (err != nil) != tt.wantErr {
					t.Errorf("rdb.Find(), NOT IN RANGE error = %q, wantErr %v\nRange: %q IP: %s", err.Error(), tt.wantErr, rangeToFind, ipToFind)
					return
				}

				if got != reasonToFind {
					t.Errorf("rdb.Find(), WRONG REASON = %q, want %q", got, reasonToFind)
					return
				}
			}

		})
	}
}

func TestClient_Remove(t *testing.T) {

	tests := []testCaseFind{}

	tests = append(tests, initTestCasesFind(100)...)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			rdb := initRDB(0)
			defer rdb.Close()

			for idx, rir := range tt.ipRanges {
				ipToFind := rir.IP
				reasonToFind := rir.Reason
				rangeToFind := rir.Range

				err := rdb.Insert(rangeToFind, reasonToFind)
				if err != nil {
					t.Errorf("rdb.Insert() error = %v, wantErr %v", err, tt.wantErr)
					t.FailNow()
				}

				if !consistent(rdb, t, rangeToFind, idx) {
					t.Errorf("rdb.Insert() error : Database INCONSISTENT after inserting range: %s", rangeToFind)
					t.FailNow()
				}
				t.Logf("rdb.Insert() Info  : Database is CONSISTENT after inserting range: %s", rangeToFind)

				got, err := rdb.Find(ipToFind)

				if err != nil {
					t.Errorf("rdb.Find(), NOT IN RANGE error = %q, wantErr %v\nRange: %q IP: %s", err.Error(), tt.wantErr, rangeToFind, ipToFind)
					return
				}

				if got != reasonToFind {
					t.Errorf("rdb.Find(), WRONG REASON = %q, want %q", got, reasonToFind)
					t.FailNow()
				}

				err = rdb.Remove(rangeToFind)

				if err != nil {
					t.Errorf("rdb.Remove(), RETURED ERROR = %q", err)
					t.FailNow()
				}

				if !consistent(rdb, t, "", 0) {
					t.Errorf("rdb.Remove() error : Database INCONSISTENT after inserting range: %s", rangeToFind)
					t.FailNow()
				}
				t.Logf("rdb.Remove() Info  : Database is CONSISTENT after inserting range: %s", rangeToFind)

				_, err = rdb.Find(ipToFind)

				// should not be found after range deletion
				if err == nil {
					t.Errorf("rdb.Find(),FOUND AFTER RANGE DELETION error = %q\nRange: %q IP: %s", err.Error(), rangeToFind, ipToFind)
					t.FailNow()
				}
			}
		})
	}
}

type testCase struct {
	name     string
	ipRanges []rangeReason
	wantErr  bool
}

// Tests whether the database is in a cosistent state.
func consistent(rdb *Client, t *testing.T, ipRange string, iteration int) bool {

	attributes, err := rdb.all()
	if err != nil {
		panic(err)
	}

	if iteration > 0 && len(attributes) <= 2 {
		panic("databse empty")
	}

	const LowerBound = 0
	const UpperBound = 1

	t.Logf("%d attributes fetched from database.", len(attributes))
	for idx, attr := range attributes {
		t.Logf("\tid=%16s idx=%4d\t%16s\tlower: %5t\tupper: %5t\t%20s", attr.ID, idx, attr.IP.String(), attr.LowerBound, attr.UpperBound, attr.Reason)
	}

	if ipRange != "" {
		low, high, err := parseRange(ipRange, "")
		if err != nil {
			panic(err)
		}

		foundLow, foundHigh := false, false
		for _, attr := range attributes {
			if attr.EqualIP(low) && attr.LowerBound {
				foundLow = true
			}

			if attr.EqualIP(high) && attr.UpperBound {
				foundHigh = true
			}
		}
		if !foundLow || !foundHigh {
			if !foundLow && !foundHigh {
				t.Errorf("did neither find inserted LOWERBOUND neither UPPERBOUND")
			} else if !foundLow {
				t.Errorf("did not find inserted LOWERBOUND")
			} else {
				t.Errorf("did not find inserted UPPERBOUND")
			}
			return false
		}
	}

	cnt := 0
	state := LowerBound
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

	if state != LowerBound {
		return false // for debugging breakpoints
	}

	return true
}

func generateBetween(low, high int64) int64 {

	if low > high {
		low, high = high, low
	}

	rand.Seed(time.Now().UnixNano())

	between := low
	if high-low > 0 {
		between = low + rand.Int63n(high-low)
	}

	return between
}

// generateRange generates a valid IP range
// and and returns a random IP that is within the range
func generateRange() (ipRange string, insideIP string) {

	const minIP = math.MaxInt32 / 1024 // don't want empty IP bytes
	const maxIP = math.MaxInt32 / 2

	const randBorder = maxIP - minIP

	rand.Seed(time.Now().UnixNano())
	low := minIP + rand.Int63n(randBorder)

	rand.Seed(time.Now().UnixNano())
	high := minIP + rand.Int63n(randBorder)

	if low > high {
		low, high = high, low
	}

	between := generateBetween(low, high)

	if between < low || high < between {
		panic("invalid ip generated")
	}

	lowIP := netaddr.MustNewIPAddress(low).String()
	highIP := netaddr.MustNewIPAddress(high).String()

	testregex := regexp.MustCompile(`[.:0-9]+`)

	if !(testregex.MatchString(lowIP) && testregex.MatchString(highIP)) {
		panic(fmt.Errorf("invalid ip generatred: low: %q high: %q", lowIP, highIP))
	}

	betweenIPStr := netaddr.MustNewIPAddress(between).String()

	hyphenRange := fmt.Sprintf("%s - %s", lowIP, highIP)

	rand.Seed(time.Now().UnixNano())

	if rand.Int()%2 == 0 {
		return hyphenRange, betweenIPStr
	}

	rand.Seed(time.Now().UnixNano())
	mask := rand.Intn(32-1) + 1

	cidrRange := fmt.Sprintf("%s/%d", lowIP, mask)

	net := netaddr.MustNewIPNetwork(cidrRange)

	lowerInt := net.First().BigInt().Int64()
	higherInt := net.Last().BigInt().Int64()

	between = generateBetween(lowerInt, higherInt)

	if between < lowerInt || higherInt < between {
		panic("invalid ip generated")
	}

	betweenIP := netaddr.MustNewIPAddress(between)

	return cidrRange, betweenIP.String()
}

func initRDB(db int) *Client {
	if db > 15 {
		panic("redis only supports database indices from 0 through 15.")
	}

	// new default client
	c, err := NewClient(Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       db,
	})
	if err != nil {
		panic(err)
	}

	// reset database
	if err := c.Reset(); err != nil {
		panic(err)
	}
	return c
}

func shuffle(seed int64, a []rangeReason) []rangeReason {
	rand.Seed(seed)
	rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })
	return a
}

func initRanges(num int) {
	// generate ranges
	for i := 0; i < num; i++ {
		ipRange, _ := generateRange()
		ranges = append(ranges, rangeReason{
			Range:  ipRange,
			Reason: fmt.Sprintf("random %5d", i),
		})
	}
}

type rangeIPReason struct {
	Range  string
	IP     string
	Reason string
}

type testCaseFind struct {
	name     string
	ipRanges []rangeIPReason
	wantErr  bool
}

var (
	findRanges = []rangeIPReason{
		{"17.115.210.3/30", "17.115.210.0", "manual 1"},
	}
)

func initRangesAndIPsWithin(num int) {
	// generate ranges
	for i := 0; i < num; i++ {
		ipRange, ip := generateRange()
		findRanges = append(findRanges, rangeIPReason{
			Range:  ipRange,
			IP:     ip,
			Reason: fmt.Sprintf("random %5d", i),
		})
	}
}

func shuffleFindTest(seed int64, a []rangeIPReason) []rangeIPReason {
	rand.Seed(seed)
	rand.Shuffle(len(a), func(i, j int) { a[i], a[j] = a[j], a[i] })
	return a
}

func initTestCasesFind(num int) (testCases []testCaseFind) {

	initRangesAndIPsWithin(100)

	testCases = make([]testCaseFind, num)

	for i := 0; i < num; i++ {

		seed := time.Now().UnixNano()

		shuffledRange := make([]rangeIPReason, len(findRanges))
		copy(shuffledRange, findRanges)
		shuffleFindTest(seed, shuffledRange)

		if i == 0 {
			shuffledRange = findRanges
		}

		testCases[i] = testCaseFind{
			name:     fmt.Sprintf("random test case find %5d", i),
			ipRanges: shuffledRange,
			wantErr:  false,
		}
	}
	return
}
