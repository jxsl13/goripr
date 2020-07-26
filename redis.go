package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

// RedisClient is an extended version of the redis.Client
type RedisClient struct {
	*redis.Client
}

// RedisOptions configures the redis database connection
type RedisOptions struct {
	// The network type, either tcp or unix.
	// Default is tcp.
	Network string
	// host:port address.
	Addr string

	// Dialer creates new network connection and has priority over
	// Network and Addr options.
	Dialer func() (net.Conn, error)

	// Hook that is called when new connection is established.
	OnConnect func(*redis.Conn) error

	// Optional password. Must match the password specified in the
	// requirepass server configuration option.
	Password string
	// Database to be selected after connecting to the server.
	DB int

	// Maximum number of retries before giving up.
	// Default is to not retry failed commands.
	MaxRetries int
	// Minimum backoff between each retry.
	// Default is 8 milliseconds; -1 disables backoff.
	MinRetryBackoff time.Duration
	// Maximum backoff between each retry.
	// Default is 512 milliseconds; -1 disables backoff.
	MaxRetryBackoff time.Duration

	// Dial timeout for establishing new connections.
	// Default is 5 seconds.
	DialTimeout time.Duration
	// Timeout for socket reads. If reached, commands will fail
	// with a timeout instead of blocking. Use value -1 for no timeout and 0 for default.
	// Default is 3 seconds.
	ReadTimeout time.Duration
	// Timeout for socket writes. If reached, commands will fail
	// with a timeout instead of blocking.
	// Default is ReadTimeout.
	WriteTimeout time.Duration

	// Maximum number of socket connections.
	// Default is 10 connections per every CPU as reported by runtime.NumCPU.
	PoolSize int
	// Minimum number of idle connections which is useful when establishing
	// new connection is slow.
	MinIdleConns int
	// Connection age at which client retires (closes) the connection.
	// Default is to not close aged connections.
	MaxConnAge time.Duration
	// Amount of time client waits for connection if all connections
	// are busy before returning an error.
	// Default is ReadTimeout + 1 second.
	PoolTimeout time.Duration
	// Amount of time after which client closes idle connections.
	// Should be less than server's timeout.
	// Default is 5 minutes. -1 disables idle timeout check.
	IdleTimeout time.Duration
	// Frequency of idle checks made by idle connections reaper.
	// Default is 1 minute. -1 disables idle connections reaper,
	// but idle connections are still discarded by the client
	// if IdleTimeout is set.
	IdleCheckFrequency time.Duration

	// TLS Config to use. When set TLS will be negotiated.
	TLSConfig *tls.Config
}

// NewRedisClient creates a new redi client connection
func NewRedisClient(options RedisOptions) (*RedisClient, error) {
	rdb := redis.NewClient(&redis.Options{
		Network:            options.Network,
		Addr:               options.Addr,
		Dialer:             options.Dialer,
		OnConnect:          options.OnConnect,
		Password:           options.Password,
		DB:                 options.DB,
		MaxRetries:         options.MaxRetries,
		MinRetryBackoff:    options.MinRetryBackoff,
		MaxRetryBackoff:    options.MaxRetryBackoff,
		DialTimeout:        options.DialTimeout,
		ReadTimeout:        options.ReadTimeout,
		WriteTimeout:       options.WriteTimeout,
		PoolSize:           options.PoolSize,
		MinIdleConns:       options.MinIdleConns,
		MaxConnAge:         options.MaxConnAge,
		PoolTimeout:        options.PoolTimeout,
		IdleTimeout:        options.IdleTimeout,
		IdleCheckFrequency: options.IdleCheckFrequency,
		TLSConfig:          options.TLSConfig,
	})

	// ping test
	result, err := rdb.Ping().Result()

	if err != nil {
		rdb.Close()
		return nil, fmt.Errorf("%w : %v", ErrConnectionFailed, err)
	}

	if result != "PONG" {
		rdb.Close()
		return nil, ErrConnectionFailed
	}

	// idempotent and important to mark these boundaries
	// we always want to have the infinite boundaries available in order to tell,
	// that there are no more elements below or above some other element.
	_, err = rdb.ZAdd(IPRangesKey,
		redis.Z{
			Score:  math.Inf(-1),
			Member: "-inf",
		},
		redis.Z{
			Score:  math.Inf(+1),
			Member: "+inf",
		},
	).Result()

	if err != nil {
		rdb.Close()
		return nil, fmt.Errorf("%w : %v", ErrDatabaseInit, err)
	}

	// type cast
	return &RedisClient{rdb}, nil

}

// InsertRangeUnsafe inserts the lower and upper bound of an IP range without doing any consistency checks
func (rdb *RedisClient) InsertRangeUnsafe(ipRange, reason string) error {
	low, high, err := Boundaries(ipRange)

	if err != nil {
		return err
	}

	lowInt, lowBits := IPToInt(low)
	highInt, highBits := IPToInt(high)

	if lowBits > IPv4Bits || highBits > IPv4Bits {
		return ErrIPv6NotSupported
	}

	return rdb.insertRangeIntUnsafe(lowInt.Int64(), highInt.Int64(), reason)
}

// insertRangeIntUnsafe does not do any range checks, allowing for a little bit more performance
func (rdb *RedisClient) insertRangeIntUnsafe(lowInt, highInt int64, reason string) error {
	uuid := generateUUID()
	lowMember, highMember := "", ""

	if lowInt == highInt {
		lowMember, highMember = uuid, uuid
	} else {
		lowMember, highMember = uuid, generateUUID()
	}

	tx := rdb.TxPipeline()

	tx.ZAdd(IPRangesKey,
		redis.Z{
			Score:  float64(lowInt),
			Member: lowMember,
		},
		redis.Z{
			Score:  float64(highInt),
			Member: highMember,
		},
	)

	tx.HMSet(lowMember, map[string]interface{}{
		"ip":     IntToIP(big.NewInt(lowInt), IPv4Bits).String(),
		"low":    true,
		"reason": reason,
	})

	tx.HMSet(highMember, map[string]interface{}{
		"ip":     IntToIP(big.NewInt(highInt), IPv4Bits).String(),
		"high":   true,
		"reason": reason,
	})

	_, err := tx.Exec()

	return err
}

// insertBoundaries does not do any range checks, allowing for a little bit more performance
// Side effect: if boundary.ID == "" -> it gets a new UUID
func (rdb *RedisClient) insertBoundaries(boundaries []*IPAttributes) error {

	tx := rdb.TxPipeline()
	// fill transaction
	for _, boundary := range boundaries {
		uuid := ""
		if boundary.ID == "" {
			uuid = generateUUID()
			boundary.ID = uuid
		} else {
			uuid = boundary.ID
		}

		intIP, ipBits := IPToInt(boundary.IP)

		if ipBits > IPv4Bits {
			return ErrIPv6NotSupported
		}

		// insert into sorted set
		tx.ZAdd(IPRangesKey,
			redis.Z{
				Score:  float64(intIP.Int64()),
				Member: uuid,
			},
		)

		if boundary.LowerBound {
			tx.HSet(uuid, "low", true)
		}

		if boundary.UpperBound {
			tx.HSet(uuid, "high", true)
		}

		tx.HSet(uuid, "reason", boundary.Reason)
	}

	//execute transaction
	_, err := tx.Exec()

	return err
}

// Insert safely inserts a new range into the database.
// Bigger ranges are sliced into smaller ranges if the Reason strings differ.
// If the reason strings are equal, ranges are expanded as expected.
func (rdb *RedisClient) Insert(ipRange, reason string) error {
	low, high, err := Boundaries(ipRange)

	if err != nil {
		return err
	}

	lowInt, lowBits := IPToInt(low)
	highInt, highBits := IPToInt(high)

	if lowBits > IPv4Bits || highBits > IPv4Bits {
		return ErrIPv6NotSupported
	}

	return rdb.insertRangeInt(lowInt.Int64(), highInt.Int64(), reason)
}

// insertRangeInt properly inserts new ranges into the database, removing other ranges, cutting them, shrinking them, etc.
func (rdb *RedisClient) insertRangeInt(lowInt64, highInt64 int64, reason string) error {

	inside, err := rdb.insideIntRange(lowInt64, highInt64)
	if err != nil {
		return err
	}

	belowLowerBound, aboveUpperBound, err := rdb.belowLowerAboveUpper(lowInt64, highInt64, 2)
	if err != nil {
		return err
	}

	// todo check if this lies outside of th erange
	belowLowerClosest := belowLowerBound[len(belowLowerBound)-1]
	aboveUpperClosest := aboveUpperBound[0]
	// todo: move cuts to their respective positions
	cutBelow := &IPAttributes{
		IP:         IntToIP(big.NewInt(lowInt64-1), IPv4Bits),
		Reason:     belowLowerClosest.Reason,
		UpperBound: true,
	}

	lowerBound := &IPAttributes{
		IP:         IntToIP(big.NewInt(lowInt64), IPv4Bits),
		Reason:     reason,
		LowerBound: true,
	}

	upperBound := &IPAttributes{
		IP:         IntToIP(big.NewInt(highInt64), IPv4Bits),
		Reason:     reason,
		UpperBound: true,
	}

	cutAbove := &IPAttributes{
		IP:         IntToIP(big.NewInt(highInt64+1), IPv4Bits),
		Reason:     aboveUpperClosest.Reason,
		LowerBound: true,
	}

	lenInside := len(inside)

	if lenInside == 0 {
		// nothin inside range

		if belowLowerClosest.UpperBound &&
			aboveUpperClosest.LowerBound {
			// case 1: set empty, infinite boundaries guarantee
			// the existence of at least one neighbour to each side
			// case 2: inserting into empty space in between other ranges
			// belowLowerClosest != aboveUpperClosest, because len(inside) == 0
			// case 3: -inf below & other range above OR other range below & +inf above
			return rdb.insertRangeIntUnsafe(lowInt64, highInt64, reason)

		} else if belowLowerClosest.LowerBound && aboveUpperClosest.UpperBound &&
			belowLowerClosest.IsSingleBoundary() && aboveUpperClosest.IsSingleBoundary() {
			// our new range is within a bigger range
			// len(inside) == 0 => outside range is connected

			newRangeBoundaries := []*IPAttributes{
				cutBelow,
				lowerBound,
				upperBound,
				cutAbove,
			}

			// sets boundaries below and above out to be inserted range
			return rdb.insertBoundaries(newRangeBoundaries)
		} else {
			panic("what is this case")
		}

	}

	// lenInside > 0

	insideMostLeft := inside[0]
	insideMostRight := inside[lenInside-1]

	if lenInside%2 == 0 {
		// even number of boundaries inside of the range

		if insideMostLeft.LowerBound && insideMostRight.UpperBound {
			// all ranges are inside of the new range.
			// meaning they are smaller and can be replaced by the new bigger range

			// delete all inside boundaries
			err = rdb.removeIDs(idsOf(inside))
			if err != nil {
				return err
			}

			// insert range
			return rdb.insertRangeIntUnsafe(lowInt64, highInt64, reason)
		}
		// ranges lie outside on both sides, meaning:
		// the mostLeft is an UpperBound, the mostRight is a lowerBound

		// delete all inside boundaries
		err = rdb.removeIDs(idsOf(inside))

		if err != nil {
			return err
		}

		newRangeBoundaries := []*IPAttributes{
			cutBelow,
			lowerBound,
			upperBound,
			cutAbove,
		}

		// insert lower cut, new range, upper cut boundary
		return rdb.insertBoundaries(newRangeBoundaries)
	}

	// lenInside % 2 == 1

	// delete all boundaries inside
	// of the new to be inserted range
	err = rdb.removeIDs(idsOf(inside))
	if err != nil {
		return err
	}

	newRangeBoundaries := []*IPAttributes{}

	if insideMostLeft.UpperBound {
		// the range at the lower end of the new range is partially
		// inside and partially outside the new range

		newRangeBoundaries = []*IPAttributes{
			cutBelow,
			lowerBound,
			upperBound,
		}

		return rdb.insertBoundaries(newRangeBoundaries)
	}

	// the range at the upper end of the new range that is to be inserted
	// is partially inside and partially outside the new range

	newRangeBoundaries = []*IPAttributes{
		lowerBound,
		upperBound,
		cutAbove,
	}

	return rdb.insertBoundaries(newRangeBoundaries)
}

// Remove removes a range from the set
func (rdb *RedisClient) Remove(ipRange string) error {
	low, high, err := Boundaries(ipRange)

	if err != nil {
		return err
	}

	lowInt, lowBits := IPToInt(low)
	highInt, highBits := IPToInt(high)

	if lowBits > IPv4Bits || highBits > IPv4Bits {
		return ErrIPv6NotSupported
	}

	lowInt64 := lowInt.Int64()
	highInt64 := highInt.Int64()

	err = rdb.insertRangeInt(lowInt64, highInt64, DeleteReason)
	if err != nil {
		return err
	}

	// get IDs of boundaries within given range
	boundaryIDs, err := rdb.insideIntIDs(lowInt64, highInt64)

	if err != nil {
		return err
	}

	// remove from sorted set and from attribute map
	return rdb.removeIDs(boundaryIDs)
}

// Inside returns all IP range boundaries that are within a given range
func (rdb *RedisClient) Inside(ipRange string) (inside []*IPAttributes, err error) {
	low, high, err := Boundaries(ipRange)

	if err != nil {
		return nil, err
	}

	lowInt, lowBits := IPToInt(low)
	highInt, highBits := IPToInt(high)

	if lowBits > IPv4Bits || highBits > IPv4Bits {
		return nil, ErrIPv6NotSupported
	}

	return rdb.insideIntRange(lowInt.Int64(), highInt.Int64())
}

// insideIntIDs returns a list of range boundary IDs that lie within lowInt64 through highInt64.
// including these two boundaries.
func (rdb *RedisClient) insideIntIDs(lowInt64, highInt64 int64) ([]string, error) {
	tx := rdb.TxPipeline()

	cmdInside := tx.ZRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
		Min: strconv.FormatInt(lowInt64, 10),
		Max: strconv.FormatInt(highInt64, 10),
	})

	_, err := tx.Exec()

	if err != nil {
		return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	insideResults, err := cmdInside.Result()
	if err != nil {
		return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	ret := make([]string, 0, len(insideResults))

	for _, result := range insideResults {
		switch t := result.Member.(type) {
		case string:
			ret = append(ret, t)
		default:
			return nil, fmt.Errorf("invalid type of member ID in method insideIntIDs: %T", result.Member)
		}
	}

	return ret, nil
}

// insideIntRange does not do any checks or ip conversions to be reusable
func (rdb *RedisClient) insideIntRange(lowInt64, highInt64 int64) (inside []*IPAttributes, err error) {
	inside = make([]*IPAttributes, 0, 3)

	tx := rdb.TxPipeline()

	cmdInside := tx.ZRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
		Min: strconv.FormatInt(lowInt64, 10),
		Max: strconv.FormatInt(highInt64, 10),
	})

	_, err = tx.Exec()

	if err != nil {
		return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	insideResults, err := cmdInside.Result()
	if err != nil {
		return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	for _, result := range insideResults {
		attr, err := rdb.fetchIPAttributes(result)
		if err != nil {
			return nil, err
		}

		inside = append(inside, attr)
	}
	return
}

// insideInfRange returns all ranges
func (rdb *RedisClient) insideInfRange() (inside []*IPAttributes, err error) {
	inside = make([]*IPAttributes, 0, 3)

	tx := rdb.TxPipeline()

	cmdInside := tx.ZRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	})

	_, err = tx.Exec()

	if err != nil {
		return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	insideResults, err := cmdInside.Result()
	if err != nil {
		return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	for _, result := range insideResults {
		attr, err := rdb.fetchIPAttributes(result)
		if err != nil {
			return nil, err
		}

		inside = append(inside, attr)
	}

	return
}

// Above returns the IP above the requested IP
func (rdb *RedisClient) Above(requestedIP string) (ip *IPAttributes, err error) {
	reqIP := net.ParseIP(requestedIP)

	if reqIP == nil {
		return nil, ErrInvalidIP
	}

	uIP, ipBits := IPToInt(reqIP)

	if ipBits > IPv4Bits {
		return nil, ErrIPv6NotSupported
	}

	tx := rdb.TxPipeline()

	cmd := tx.ZRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
		Min:    strconv.FormatInt(uIP.Int64(), 10),
		Max:    "+inf",
		Offset: 0,
		Count:  1,
	})

	_, err = tx.Exec()

	if err != nil {
		return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	results, err := cmd.Result()

	if err != nil {
		return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	if len(results) < 1 {
		return nil, ErrNoResult
	}

	aboveResult := results[0]

	return rdb.fetchIPAttributes(aboveResult)
}

// Below returns the range delimiting IP that is directly below the requestedIP
func (rdb *RedisClient) Below(requestedIP string) (ip *IPAttributes, err error) {
	reqIP := net.ParseIP(requestedIP)

	if reqIP == nil {
		return nil, ErrInvalidIP
	}

	uIP, ipBits := IPToInt(reqIP)

	if ipBits > IPv4Bits {
		return nil, ErrIPv6NotSupported
	}

	tx := rdb.TxPipeline()

	cmd := tx.ZRevRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatInt(uIP.Int64(), 10),
		Offset: 0,
		Count:  1,
	})

	_, err = tx.Exec()

	if err != nil {
		return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	results, err := cmd.Result()

	if err != nil {
		return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	if len(results) < 1 {
		return nil, ErrNoResult
	}

	belowResult := results[0]
	return rdb.fetchIPAttributes(belowResult)
}

func (rdb *RedisClient) belowLowerAboveUpper(lower, upper, num int64) (belowLower, aboveUpper []*IPAttributes, err error) {

	tx := rdb.TxPipeline()

	cmdBelowLower := tx.ZRevRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatInt(lower-1, 10),
		Offset: 0,
		Count:  num,
	})

	cmdAboveUpper := tx.ZRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
		Min:    strconv.FormatInt(upper+1, 10),
		Max:    "+inf",
		Offset: 0,
		Count:  num,
	})

	_, err = tx.Exec()
	if err != nil {
		return nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	belowLowerResults, err := cmdBelowLower.Result()

	// inverse slice to have the order from -inf .... +inf
	for i, j := 0, len(belowLowerResults)-1; i < j; i, j = i+1, j-1 {
		belowLowerResults[i], belowLowerResults[j] = belowLowerResults[j], belowLowerResults[i]
	}

	if err != nil {
		return nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	aboveUpperResults, err := cmdAboveUpper.Result()
	if err != nil {
		return nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	belowLower, err = rdb.fetchAllIPAttributes(belowLowerResults...)
	if err != nil {
		return nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	aboveUpper, err = rdb.fetchAllIPAttributes(aboveUpperResults...)
	if err != nil {
		return nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	return
}

// Neighbours returns numNeighbours IPs that are above and numNeighbours IPs that are below the requestedIP
func (rdb *RedisClient) Neighbours(requestedIP string, numNeighbours uint) (below, above []*IPAttributes, err error) {
	reqIP := net.ParseIP(requestedIP)

	if reqIP == nil {
		return nil, nil, ErrInvalidIP
	}

	bIP, ipBits := IPToInt(reqIP)

	if ipBits > IPv4Bits {
		return nil, nil, ErrIPv6NotSupported
	}

	return rdb.neighboursInt(bIP.Int64(), numNeighbours)
}

// neighboursInt does not do any checks, thus making it reusable in other methods without check overhead
func (rdb *RedisClient) neighboursInt(ofIP int64, numNeighbours uint) (below, above []*IPAttributes, err error) {

	below = make([]*IPAttributes, 0, numNeighbours)
	above = make([]*IPAttributes, 0, numNeighbours)

	tx := rdb.TxPipeline()

	cmdBelow := tx.ZRevRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
		Min:    "-inf",
		Max:    strconv.FormatInt(ofIP, 10),
		Offset: 0,
		Count:  int64(numNeighbours),
	})

	cmdAbove := tx.ZRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
		Min:    strconv.FormatInt(ofIP, 10),
		Max:    "+inf",
		Offset: 0,
		Count:  int64(numNeighbours),
	})

	_, err = tx.Exec()

	if err != nil {
		return nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	belowResults, err := cmdBelow.Result()

	if err != nil {
		return nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	for _, result := range belowResults {
		attr, err := rdb.fetchIPAttributes(result)
		if errors.Is(err, ErrLowerBoundary) {
			attr = GlobalLowerBoundary
		} else if err != nil {
			return nil, nil, err
		}

		// prepend for correct order
		below = append([]*IPAttributes{attr}, below...)
	}

	aboveResults, err := cmdAbove.Result()

	if err != nil {
		return nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	for _, result := range aboveResults {
		attr, err := rdb.fetchIPAttributes(result)
		if errors.Is(err, ErrUpperBoundary) {
			attr = GlobalUpperBoundary
		} else if err != nil {
			return nil, nil, err
		}

		above = append(above, attr)
	}

	return
}

// fetchIpAttributes gets the remaining IP related attributes that belong to the IP range boundary
// that is encoded in the redis.Z.Score attribute
func (rdb *RedisClient) fetchIPAttributes(result redis.Z) (*IPAttributes, error) {

	switch result.Score {
	case math.Inf(-1):
		return GlobalLowerBoundary, nil
	case math.Inf(1):
		return GlobalUpperBoundary, nil
	}

	id := ""
	resultIP := net.IP{}

	switch t := result.Member.(type) {
	case string:
		id = t
		uIP := big.NewInt(int64(result.Score))
		resultIP = IntToIP(uIP, IPv4Bits)
	default:
		return nil, fmt.Errorf("%w : member result is not of type string : %T", ErrNoResult, t)
	}

	fields, err := rdb.HMGet(id, "low", "high", "reason").Result()

	if err != nil || len(fields) == 0 {
		return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	low := false
	switch t := fields[0].(type) {
	case string:
		low = t != "0"
	case bool:
		low = t
	case int:
		low = t != 0
	case nil:
		low = false
	default:
		return nil, fmt.Errorf("%w : 'low' type unknown : %T", ErrNoResult, t)
	}

	high := false
	switch t := fields[1].(type) {
	case string:
		high = t != "0"
	case bool:
		high = t
	case int:
		high = t != 0
	case nil:
		high = false
	default:
		return nil, fmt.Errorf("%w : 'high' type unknown : %T", ErrNoResult, t)
	}

	reason := ""
	switch t := fields[2].(type) {
	case string:
		reason = t
	default:
		return nil, fmt.Errorf("%w : 'reason' type unknown : %T", ErrNoResult, t)
	}

	return &IPAttributes{
		ID:         id,
		IP:         resultIP,
		Reason:     reason,
		LowerBound: low,
		UpperBound: high,
	}, nil
}

// fetch a list of IPAttributes passed as result parameters
func (rdb *RedisClient) fetchAllIPAttributes(results ...redis.Z) ([]*IPAttributes, error) {

	ipAttributes := make([]*IPAttributes, 0, len(results))

	for _, result := range results {
		switch result.Score {
		case math.Inf(-1):
			ipAttributes = append(ipAttributes, GlobalLowerBoundary)
			continue
		case math.Inf(1):
			ipAttributes = append(ipAttributes, GlobalUpperBoundary)
			continue
		}

		id := ""
		resultIP := net.IP{}

		switch t := result.Member.(type) {
		case string:
			id = t
			uIP := big.NewInt(int64(result.Score))
			resultIP = IntToIP(uIP, IPv4Bits)
		default:
			return nil, fmt.Errorf("%w : member result is not of type string : %T", ErrNoResult, t)
		}

		fields, err := rdb.HMGet(id, "low", "high", "reason").Result()

		if err != nil || len(fields) == 0 {
			return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		low := false
		switch t := fields[0].(type) {
		case string:
			low = t != "0"
		case bool:
			low = t
		case int:
			low = t != 0
		case nil:
			low = false
		default:
			return nil, fmt.Errorf("%w : 'low' type unknown : %T", ErrNoResult, t)
		}

		high := false
		switch t := fields[1].(type) {
		case string:
			high = t != "0"
		case bool:
			high = t
		case int:
			high = t != 0
		case nil:
			high = false
		default:
			return nil, fmt.Errorf("%w : 'high' type unknown : %T", ErrNoResult, t)
		}

		reason := ""
		switch t := fields[2].(type) {
		case string:
			reason = t
		default:
			return nil, fmt.Errorf("%w : 'reason' type unknown : %T", ErrNoResult, t)
		}

		ipAttributes = append(ipAttributes, &IPAttributes{
			ID:         id,
			IP:         resultIP,
			Reason:     reason,
			LowerBound: low,
			UpperBound: high,
		})
	}

	return ipAttributes, nil
}

func (rdb *RedisClient) removeIDs(ids []string) error {
	tx := rdb.TxPipeline()

	// remove from sorted set
	tx.ZRem(IPRangesKey, ids)

	// remove attribute object
	tx.Del(ids...)

	_, err := tx.Exec()
	if err != nil {
		return err
	}
	return nil
}

// removes dubplicates that are next to each other.
func idsOf(attributes []*IPAttributes) []string {
	ids := make([]string, 0, len(attributes))

	for idx, attr := range attributes {
		// attributes are sorted
		if idx > 0 && len(attr.ID) > 0 && attr.ID == attributes[idx-1].ID {
			// skip continuous dubplicates
			continue
		}
		if attr.ID != "" {
			ids = append(ids, attr.ID)
		}
	}
	return ids
}

// InAnyRange returns a non empty reason and nil for an error if the given IP is
// found within any previously inserted IP range.
// An error is returned if the request fails and thus is false.
func (rdb *RedisClient) InAnyRange(ip string) (string, error) {
	reqIP := net.ParseIP(ip)

	if reqIP == nil {
		return "", ErrInvalidIP
	}

	uIP, ipBits := IPToInt(reqIP)

	if ipBits > IPv4Bits {
		return "", ErrIPv6NotSupported
	}

	belowN, aboveN, err := rdb.neighboursInt(uIP.Int64(), 1)
	if err != nil {
		return "", err
	}

	// this is enforced by the idempotent database initialization.
	below, above := belowN[0], aboveN[0]

	inRange := below.LowerBound && !below.UpperBound &&
		!above.LowerBound && above.UpperBound

	if below.Reason != above.Reason {
		panic(fmt.Errorf(" '%s'.Reason != '%s'.Reason : '%s' != '%s'", below.ID, above.ID, below.Reason, above.Reason))
	}

	if !inRange {
		return "", ErrIPNotFoundInAnyRange
	}

	return below.Reason, nil

}