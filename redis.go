package goripr

import (
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/go-redis/redis"
	"github.com/xgfone/netaddr"
)

var (
	customIPRangeRegex = regexp.MustCompile(`([0-9a-f:.]{7,41})\s*-\s*([0-9a-f:.]{7,41})`)
)

// Options configures the redis database connection
type Options struct {
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

// Client is an extended version of the redis.Client
type Client struct {
	rdb *redis.Client
	mu  sync.RWMutex
}

// NewClient creates a new redi client connection
func NewClient(options Options) (*Client, error) {
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

	client := &Client{
		rdb: rdb,
	}

	err = client.init()
	if err != nil {
		client.Close()
		return nil, fmt.Errorf("%w : %v", ErrDatabaseInit, err)
	}

	return client, nil
}

// init the GlobalBoundaries
func (c *Client) init() error {
	// idempotent and important to mark these boundaries
	// we always want to have the infinite boundaries available in order to tell,
	// that there are no more elements below or above some other element.
	tx := c.rdb.TxPipeline()

	tx.ZAdd(IPRangesKey,
		redis.Z{
			Score:  math.Inf(-1),
			Member: "-inf",
		},
		redis.Z{
			Score:  math.Inf(+1),
			Member: "+inf",
		},
	)

	tx.HMSet("-inf", map[string]interface{}{
		"low":    false,
		"high":   true,
		"reason": "-inf",
	})

	tx.HMSet("+inf", map[string]interface{}{
		"low":    true,
		"high":   false,
		"reason": "+inf",
	})
	_, err := tx.Exec()

	return err
}

// Close the redis database connection
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Flush removes all of the database content including the global bounadaries.
func (c *Client) Flush() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, err := c.rdb.FlushDB().Result()
	return err
}

// Reset the database except for its global boundaries
func (c *Client) Reset() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, err := c.rdb.FlushDB().Result(); err != nil {
		return err
	}
	return c.init()
}

// all retrieves all range boundaries that are within the database.
func (c *Client) all() (inside []boundary, err error) {

	results, err := c.rdb.ZRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
		Min: "-inf",
		Max: "+inf",
	}).Result()

	if err != nil {
		return nil, err
	}

	for _, result := range results {
		bnd := newBoundary(result.Score, "", false, false)
		inside = append(inside, bnd)
	}

	tx := c.rdb.TxPipeline()

	cmds := make([]*redis.SliceCmd, 0, len(inside))
	for _, bnd := range inside {
		cmd := bnd.Get(tx)
		cmds = append(cmds, cmd)
	}

	_, err = tx.Exec()
	if err != nil {
		return nil, err
	}

	for idx, cmd := range cmds {
		result, err := cmd.Result()
		if err != nil {
			return nil, err
		}

		if len(result) != 3 {
			panic("database inconsistent")
		}

		low := false
		switch t := result[0].(type) {
		case string:
			low = t == "1"
		default:
			low = false
		}

		high := false
		switch t := result[1].(type) {
		case string:
			high = t == "1"
		default:
			high = false
		}

		reason := ""
		switch t := result[2].(type) {
		case string:
			reason = t
		default:
			reason = ""
		}

		inside[idx].LowerBound = low
		inside[idx].UpperBound = high
		inside[idx].Reason = reason
	}

	sort.Sort(byIP(inside))
	return inside, nil
}

// neighboursInt does not do any checks, thus making it reusable in other methods without check overhead
func (c *Client) vicinity(low, high boundary, num int64) (below, inside, above []boundary, err error) {

	if num < 0 {
		panic("passed num parameter has to be >= 0")
	}

	below = make([]boundary, 0, num)
	inside = make([]boundary, 0, 1)
	above = make([]boundary, 0, num)

	tx := c.rdb.TxPipeline()

	cmdBelow := tx.ZRevRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
		Min:    "-inf",
		Max:    low.Below().Int64String(),
		Offset: 0,
		Count:  num,
	})

	cmdInside := tx.ZRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
		Min: low.Int64String(),
		Max: high.Int64String(),
	})

	cmdAbove := tx.ZRangeByScoreWithScores(IPRangesKey, redis.ZRangeBy{
		Min:    high.Above().Int64String(),
		Max:    "+inf",
		Offset: 0,
		Count:  num,
	})

	_, err = tx.Exec()

	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	// transaction results of below command
	belowResults, err := cmdBelow.Result()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	// create below IPs
	for _, result := range belowResults {
		bnd := newBoundary(result.Score, "", false, false)
		below = append(below, bnd)
	}

	// should be faster than prepending values to a slice
	sort.Sort(byIP(below))

	insideResults, err := cmdInside.Result()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	// create inside IPs
	for _, result := range insideResults {
		boundary := newBoundary(result.Score, "", false, false)
		inside = append(inside, boundary)
	}

	sort.Sort(byIP(inside))

	aboveResults, err := cmdAbove.Result()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	// create above IPs
	for _, result := range aboveResults {
		bnd := newBoundary(result.Score, "", false, false)
		above = append(above, bnd)
	}

	sort.Sort(byIP(above))

	// at this point above, inside and below each contain not yet fully filled boundaries
	// they are still missing their reason, lower and upper bound information

	tx = c.rdb.TxPipeline()

	belowAttrCmds := make([]*redis.SliceCmd, 0, len(below))
	for _, bnd := range below {
		belowAttrCmds = append(belowAttrCmds, tx.HMGet(bnd.ID, "low", "high", "reason"))
	}

	insideAttrCmds := make([]*redis.SliceCmd, 0, len(inside))
	for _, bnd := range inside {
		insideAttrCmds = append(insideAttrCmds, tx.HMGet(bnd.ID, "low", "high", "reason"))
	}

	aboveAttrCmds := make([]*redis.SliceCmd, 0, len(above))
	for _, bnd := range above {
		aboveAttrCmds = append(aboveAttrCmds, tx.HMGet(bnd.ID, "low", "high", "reason"))
	}

	_, err = tx.Exec()
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	for idx, cmd := range belowAttrCmds {
		result, err := cmd.Result()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		if len(result) != 3 {
			err = fmt.Errorf("expected 3 result attributes, got %d", len(result))
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		low := false
		switch t := result[0].(type) {
		case string:
			low = t == "1"
		case nil:
			low = false
		default:
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, fmt.Errorf("unexpected type: %T", t))
		}

		high := false
		switch t := result[1].(type) {
		case string:
			high = t == "1"
		case nil:
			high = false
		default:
			err = fmt.Errorf("unexpected type: %T", t)
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		reason := ""
		switch t := result[2].(type) {
		case string:
			reason = t
		default:
			err = fmt.Errorf("unexpected type: %T", t)
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		below[idx].LowerBound = low
		below[idx].UpperBound = high
		below[idx].Reason = reason
	}

	for idx, cmd := range insideAttrCmds {
		result, err := cmd.Result()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		if len(result) != 3 {
			err = fmt.Errorf("expected 3 result attributes, got %d", len(result))
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		low := false
		switch t := result[0].(type) {
		case string:
			low = t == "1"
		case nil:
			low = false
		default:
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, fmt.Errorf("unexpected type: %T", t))
		}

		high := false
		switch t := result[1].(type) {
		case string:
			high = t == "1"
		case nil:
			high = false
		default:
			err = fmt.Errorf("unexpected type: %T", t)
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		reason := ""
		switch t := result[2].(type) {
		case string:
			reason = t
		default:
			err = fmt.Errorf("unexpected type: %T", t)
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		inside[idx].LowerBound = low
		inside[idx].UpperBound = high
		inside[idx].Reason = reason
	}

	for idx, cmd := range aboveAttrCmds {
		result, err := cmd.Result()
		if err != nil {
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		if len(result) != 3 {
			err = fmt.Errorf("expected 3 result attributes, got %d", len(result))
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		low := false
		switch t := result[0].(type) {
		case string:
			low = t == "1"
		case nil:
			low = false
		default:
			err = fmt.Errorf("unexpected type: %T", t)
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		high := false
		switch t := result[1].(type) {
		case string:
			high = t == "1"
		case nil:
			high = false
		default:
			err = fmt.Errorf("unexpected type: %T", t)
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		reason := ""
		switch t := result[2].(type) {
		case string:
			reason = t
		default:
			err = fmt.Errorf("unexpected type: %T", t)
			return nil, nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		above[idx].LowerBound = low
		above[idx].UpperBound = high
		above[idx].Reason = reason
	}

	return below, inside, above, nil
}

// Insert inserts a new IP range or IP into the database with an associated reason string
func (c *Client) Insert(ipRange, reason string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	low, high, err := parseRange(ipRange, reason)
	if err != nil {
		return err
	}

	tx := c.rdb.TxPipeline()

	belowN, inside, aboveN, err := c.vicinity(low, high, 1)
	if err != nil {
		return err
	}

	if len(belowN) < 1 || len(aboveN) < 1 {
		panic(ErrDatabaseInconsistent)
	}

	// remove inside
	for _, bnd := range inside {
		bnd.Remove(tx)
	}

	belowNearest := belowN[0]
	aboveNearest := aboveN[0]

	belowCut := low.Below()
	belowCut.SetUpperBound()
	belowCut.Reason = belowNearest.Reason

	aboveCut := high.Above()
	aboveCut.SetLowerBound()
	aboveCut.Reason = aboveNearest.Reason

	insertLowerBound := true
	insertUpperBound := true

	if belowNearest.IsLowerBound() {
		// need to cut below
		if !belowNearest.EqualIP(belowCut) {
			// can cut below |----
			if !belowNearest.EqualReason(low) {
				// only insert if reasons differ
				belowCut.Insert(tx)
			} else {
				// extend range towards belowNearest
				insertLowerBound = false
			}
		} else {
			// cannot cut below
			if !belowNearest.EqualReason(low) {
				// if reasons differ, make beLowNearest a single bound
				belowNearest.SetDoubleBound()
				belowNearest.Insert(tx)
			} else {
				insertLowerBound = false
			}
		}
	} else if belowNearest.IsDoubleBound() && belowNearest.EqualIP(belowCut) && belowNearest.EqualReason(low) {
		// one IP below we have a single boundary range with the same reason
		belowNearest.SetLowerBound()
		belowNearest.Insert(tx)
	}

	if aboveNearest.IsUpperBound() {
		// need to cut above
		if !aboveNearest.EqualIP(aboveCut) {
			// can cut above -----|
			if !aboveNearest.EqualReason(high) {
				// insert if reasons differ
				aboveCut.Insert(tx)
			} else {
				// don't insert, because extends range
				// to upperbound above
				insertUpperBound = false
			}

		} else {
			// cannot cut above
			if !aboveNearest.EqualReason(high) {
				aboveNearest.SetDoubleBound()
				aboveNearest.Insert(tx)
			} else {
				insertUpperBound = false
			}
		}
	} else if aboveNearest.IsDoubleBound() && aboveNearest.EqualIP(aboveCut) && aboveNearest.EqualReason(high) {
		// one IP above we have a single boundary range with the same reason
		aboveNearest.SetUpperBound()
		aboveNearest.Insert(tx)
	}

	if low.EqualIP(high) && insertLowerBound && insertUpperBound {
		doubleBoundary := low
		doubleBoundary.SetDoubleBound()
		doubleBoundary.Insert(tx)
	} else if insertLowerBound && insertUpperBound {
		low.Insert(tx)
		high.Insert(tx)
	} else if insertLowerBound {
		low.Insert(tx)
	} else if insertUpperBound {
		high.Insert(tx)
	}

	_, err = tx.Exec()
	return err
}

// Remove removes an IP range from the database.
func (c *Client) Remove(ipRange string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	low, high, err := parseRange(ipRange, "")

	if err != nil {
		return err
	}

	tx := c.rdb.TxPipeline()

	below, inside, above, err := c.vicinity(low, high, 1)
	if err != nil {
		return err
	}

	for _, bnd := range inside {
		bnd.Remove(tx)
	}

	belowNearest := below[0]
	aboveNearest := above[0]

	belowCut := low.Below()
	belowCut.SetUpperBound()
	belowCut.Reason = belowNearest.Reason

	aboveCut := high.Above()
	aboveCut.SetUpperBound()
	aboveCut.Reason = aboveNearest.Reason

	if belowNearest.IsLowerBound() {
		// need to cut below
		if !belowNearest.EqualIP(belowCut) {
			// can cut
			belowCut.Insert(tx)
		} else {
			// cannot cut
			belowNearest.SetDoubleBound()
			belowNearest.Insert(tx)
		}
	}

	if aboveNearest.IsUpperBound() {
		// need to cut above
		if !aboveNearest.EqualIP(aboveCut) {
			// can cut above
			aboveCut.Insert(tx)
		} else {
			// cannot cut above
			aboveNearest.SetDoubleBound()
			aboveNearest.Insert(tx)

		}
	}

	_, err = tx.Exec()
	return err
}

// Find searches for the requested IP in the database. If the IP is found within any previously inserted range,
// the associated reason is returned. If it is not found, an error is returned instead.
func (c *Client) Find(ip string) (reason string, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	ipaddr, err := netaddr.NewIPAddress(ip, 4)
	if err != nil {
		return "", fmt.Errorf("%w : %v", ErrInvalidIP, err)
	}
	bnd := newBoundary(ipaddr.IP(), "", true, true)

	below, inside, above, err := c.vicinity(bnd, bnd, 1)
	if err != nil {
		return "", err
	}

	if len(inside) == 1 {
		found := inside[0]
		return found.Reason, nil
	}

	belowNearest := below[0]
	aboveNearest := above[0]

	if belowNearest.IsLowerBound() && aboveNearest.IsUpperBound() {
		if belowNearest.EqualReason(aboveNearest) {
			return belowNearest.Reason, nil
		}
		panic("reasons inconsistent")
	}

	return "", ErrIPNotFound
}

func parseRange(r, reason string) (low, high boundary, err error) {
	ip, err := netaddr.NewIPAddress(r, 4)
	if err == nil {
		r := newBoundary(ip.IP(), reason, true, true)
		return r, r, nil
	}
	// parsing as IP failed

	net, err := netaddr.NewIPNetwork(r)
	if err == nil {
		low = newBoundary(net.First().IP(), reason, true, false)
		high = newBoundary(net.Last().IP(), reason, false, true)
		return low, high, nil
	}
	// parsing as cidr failed x.x.x.x/24

	var dummy boundary
	if matches := customIPRangeRegex.FindStringSubmatch(r); len(matches) == 3 {
		lowerBound := matches[1]
		upperBound := matches[2]

		lowIP, err := netaddr.NewIPAddress(lowerBound)
		if err != nil {
			return dummy, dummy, fmt.Errorf("%w : %v", ErrInvalidRange, err)
		}
		highIP, err := netaddr.NewIPAddress(upperBound)
		if err != nil {
			return dummy, dummy, fmt.Errorf("%w : %v", ErrInvalidRange, err)
		}

		if lowIP.Compare(highIP) > 0 {
			return dummy, dummy, ErrInvalidRange
		}

		low = newBoundary(lowIP.IP(), reason, true, false)
		high = newBoundary(highIP.IP(), reason, false, true)
		return low, high, nil
	}
	return dummy, dummy, ErrInvalidRange
}

// UpdateFunc updates the previous reason to a new reason.
type UpdateFunc func(oldReason string) (newReason string)

// UpdateReasonOf updates the reason of the range that contains the passed ip.
func (c *Client) UpdateReasonOf(ip string, fn UpdateFunc) (err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ipaddr, err := netaddr.NewIPAddress(ip, 4)
	if err != nil {
		return fmt.Errorf("%w : %v", ErrInvalidIP, err)
	}
	bnd := newBoundary(ipaddr.IP(), "", true, true)

	below, inside, above, err := c.vicinity(bnd, bnd, 1)
	if err != nil {
		return err
	}

	// must exist, because of +-inf boundaries
	belowNearest := below[0]
	aboveNearest := above[0]

	tx := c.rdb.TxPipeline()

	if len(inside) == 1 {
		found := inside[0]

		// needs to be updates in all cases
		found.Reason = fn(found.Reason)

		// we either hit a double boundary, a lower or an upper boundary
		if found.IsDoubleBound() {
			// hit single ip range
			found.Update(tx)
		} else if found.IsLowerBound() {
			if aboveNearest.IsUpperBound() {
				// lower bound
				found.Update(tx)

				// upper bound
				aboveNearest.Reason = fn(aboveNearest.Reason)
				aboveNearest.Update(tx)
			} else {
				panic("database inconsistent")
			}
		} else {
			// upperbound
			if belowNearest.IsLowerBound() {

				// lower bound
				belowNearest.Reason = fn(aboveNearest.Reason)
				belowNearest.Update(tx)

				// upper bound
				found.Insert(tx)
			} else {
				panic("database inconsistent")
			}
		}

		_, err = tx.Exec()
		return err
	}

	// len(inside) == 0
	// anything else should logically be impossible

	if belowNearest.IsLowerBound() && aboveNearest.IsUpperBound() {
		if belowNearest.EqualReason(aboveNearest) {
			belowNearest.Reason = fn(belowNearest.Reason)
			aboveNearest.Reason = fn(aboveNearest.Reason)

			belowNearest.Update(tx)
			aboveNearest.Update(tx)

			_, err = tx.Exec()
			return err
		}
		panic("database reasons inconsistent")
	}

	return ErrIPNotFound
}
