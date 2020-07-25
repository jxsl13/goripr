package main

import (
	"crypto/tls"
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

	// idempotent
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

	uuid := generateUUID()
	lowMember, highMember := "", ""

	if low.Equal(high) {
		lowMember, highMember = uuid, uuid
	} else {
		lowMember, highMember = uuid, generateUUID()
	}

	tx := rdb.TxPipeline()

	tx.ZAdd(IPRangesKey,
		redis.Z{
			Score:  float64(lowInt.Int64()),
			Member: lowMember,
		},
		redis.Z{
			Score:  float64(highInt.Int64()),
			Member: highMember,
		},
	)

	tx.HMSet(lowMember, map[string]interface{}{
		"low":    true,
		"reason": reason,
	})

	tx.HMSet(highMember, map[string]interface{}{
		"high":   true,
		"reason": reason,
	})

	_, err = tx.Exec()

	return err
}

// IPAbove returns the IP above the requested IP
func (rdb *RedisClient) IPAbove(requestedIP string) (ip *IPAttributes, err error) {
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

	if aboveResult.Score == math.Inf(1) {
		return nil, ErrUpperBoundary
	}

	id := ""
	resultIP := net.IP{}

	switch t := aboveResult.Member.(type) {
	case string:
		id = t
		uIP := big.NewInt(int64(aboveResult.Score))
		resultIP = IntToIP(uIP, IPv6Bits)
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

// IPBelow returns the range delimiting IP that is directly below the requestedIP
func (rdb *RedisClient) IPBelow(requestedIP string) (ip *IPAttributes, err error) {
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

	if belowResult.Score == math.Inf(-1) {
		return nil, ErrLowerBoundary
	}

	id := ""
	resultIP := net.IP{}

	switch t := belowResult.Member.(type) {
	case string:
		id = t
		uIP := big.NewInt(int64(belowResult.Score))
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

// func aboveIP(rdb *redis.Client, ip string) (*ZMember, error) {

// 	netIP := net.ParseIP(ip)

// 	if netIP == nil {
// 		return nil, fmt.Errorf("invalid IP passed: %s", ip)
// 	}

// 	intIP, _ := IPToInt(netIP)

// 	tx := rdb.TxPipeline()

// 	cmd := tx.ZRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
// 		Min:    strconv.FormatInt(intIP.Int64(), 10),
// 		Max:    "+inf",
// 		Offset: 0,
// 		Count:  1,
// 	})

// 	_, err := tx.Exec()
// 	if err != nil {
// 		return nil, fmt.Errorf("%w: %v", ErrIPNotFoundAbove, err)
// 	}

// 	results, err := cmd.Result()
// 	if err != nil {
// 		return nil, fmt.Errorf("%w: %v", ErrIPNotFoundAbove, err)
// 	}

// 	if len(results) < 1 {
// 		return nil, ErrIPNotFoundAbove
// 	}

// 	zm := ZMember{}
// 	err = zm.FromZ(results[0])
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &zm, nil
// }

// func belowIP(rdb *redis.Client, ip string) (*ZMember, error) {
// 	netIP := net.ParseIP(ip)

// 	if netIP == nil {
// 		return nil, fmt.Errorf("invalid IP passed: %s", ip)
// 	}

// 	intIP, _ := IPToInt(netIP)

// 	tx := rdb.TxPipeline()

// 	cmd := tx.ZRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
// 		Min:    strconv.FormatInt(intIP.Int64(), 10),
// 		Max:    "+inf",
// 		Offset: 0,
// 		Count:  1,
// 	})

// 	_, err := tx.Exec()
// 	if err != nil {
// 		return nil, fmt.Errorf("%w: %v", ErrIPNotFoundAbove, err)
// 	}

// 	results, err := cmd.Result()
// 	if err != nil {
// 		return nil, fmt.Errorf("%w: %v", ErrIPNotFoundAbove, err)
// 	}

// 	if len(results) < 1 {
// 		return nil, ErrIPNotFoundAbove
// 	}

// 	zm := ZMember{}
// 	err = zm.FromZ(results[0])
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &zm, nil
// }
