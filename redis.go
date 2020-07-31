package goripr

import (
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/go-redis/redis"
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
		rdb,
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
	_, err := c.rdb.ZAdd(IPRangesKey,
		redis.Z{
			Score:  math.Inf(-1),
			Member: "-inf",
		},
		redis.Z{
			Score:  math.Inf(+1),
			Member: "+inf",
		},
	).Result()

	return err
}

// Close the redis database connection
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Flush removes all of the database content including the global bounadaries.
func (c *Client) Flush() error {
	_, err := c.rdb.FlushDB().Result()
	return err
}

// Reset the database except for its global boundaries
func (c *Client) Reset() error {

	if err := c.Flush(); err != nil {
		return err
	}
	return c.init()
}

// insertBoundaries does not do any range checks, allowing for a little bit more performance
// Side effect: if boundary.ID == "" -> it gets a new UUID
func (c *Client) insertBoundaries(boundaries []*ipAttributes) error {

	tx := c.rdb.TxPipeline()
	// fill transaction
	for _, boundary := range boundaries {
		id := ""
		if boundary.ID == "" {

			id = boundary.IP.String()
			boundary.ID = id
		} else {
			id = boundary.ID
		}

		intIP, err := ipToInt64(boundary.IP)
		if err != nil {
			return err
		}

		// insert into sorted set
		tx.ZAdd(IPRangesKey,
			redis.Z{
				Score:  float64(intIP),
				Member: id,
			},
		)

		if boundary.LowerBound {
			tx.HSet(id, "low", true)
		}

		if boundary.UpperBound {
			tx.HSet(id, "high", true)
		}

		tx.HSet(id, "reason", boundary.Reason)
	}

	//execute transaction
	_, err := tx.Exec()

	return err
}

// Insert safely inserts a new range into the database.
// Bigger ranges are sliced into smaller ranges if the Reason strings differ.
// If the reason strings are equal, ranges are expanded as expected.
func (c *Client) Insert(ipRange, reason string) error {
	low, high, err := boundaries(ipRange)

	if err != nil {
		return err
	}

	lowInt64, _ := ipToInt64(low)
	highInt64, _ := ipToInt64(high)

	if lowInt64 == highInt64 {
		// edge case, range is single value
		return c.insertSingleInt(lowInt64, reason)
	}

	return c.insertRangeInt(lowInt64, highInt64, reason)
}

func (c *Client) insertSingleInt(singleInt int64, reason string) error {

	below, above, err := c.neighboursInt(singleInt, 1)
	if err != nil {
		return err
	}

	closestBelow := below[len(below)-1]
	closestAbove := above[0]

	ip, err := int64ToIP(singleInt)
	if err != nil {
		return err
	}

	singleBoundary := &ipAttributes{
		IP:         ip,
		Reason:     reason,
		LowerBound: true,
		UpperBound: true,
	}

	if closestBelow.Equal(closestAbove) {

		// hittig an edge / a boundary directly
		hitBoundary := closestBelow

		// remove the hit boundary
		err := c.removeIDs(hitBoundary.ID)
		if err != nil {
			return err
		}

		if !hitBoundary.IsSingleBoundary() {
			// hit a single value range
			// simply replace it

			newRange := []*ipAttributes{
				singleBoundary,
			}

			return c.insertBoundaries(newRange)

		} else if hitBoundary.LowerBound {
			// must be single boundary, meaning a range with at least two members

			ip, err := int64ToIP(hitBoundary.IPInt64() + 1)
			if err != nil {
				return err
			}

			cutAbove := &ipAttributes{
				IP:         ip,
				Reason:     hitBoundary.Reason,
				LowerBound: true,
			}

			// default case
			newRange := []*ipAttributes{
				singleBoundary,
				cutAbove,
			}

			boundaries, err := c.fetchBoundaries(cutAbove.IP)
			if err != nil {
				return err
			}

			hitCutAbove := (*ipAttributes)(nil)
			if len(boundaries) == 1 {
				hitCutAbove = boundaries[0]
			}

			if hitCutAbove != nil {
				// when we try to cut the range above, but hit the upper boundary
				// the upper boundary becomes a single value range
				if !hitCutAbove.UpperBound {
					panic("Database inconsistent!")
				}
				// must be an upper boundary

				// shrink range above to single value range
				_, err := c.rdb.HSet(hitCutAbove.ID, "low", true).Result()
				if err != nil {
					return err
				}

				// remove cut, as there is no cutting of the range above
				// needed anymore.
				newRange = []*ipAttributes{
					singleBoundary,
				}
			}

			return c.insertBoundaries(newRange)

		} else {
			// hitBoundary.UpperBound
			ip, err := int64ToIP(hitBoundary.IPInt64() - 1)
			if err != nil {
				return err
			}
			cutBelow := &ipAttributes{
				IP:         ip,
				Reason:     hitBoundary.Reason,
				UpperBound: true,
			}

			boundaries, err := c.fetchBoundaries(cutBelow.IP)
			if err != nil {
				return err
			}

			hitCutBelow := (*ipAttributes)(nil)
			if len(boundaries) == 1 {
				hitCutBelow = boundaries[0]
			}

			// default case
			newRange := []*ipAttributes{
				cutBelow,
				singleBoundary,
			}

			// case in which the range only contains two values
			if hitCutBelow != nil {
				// when we try to cut the range below, but hit the lower boundary
				// the lower boundary becomes a single value range
				if !hitCutBelow.LowerBound {
					panic("Database inconsistent!")
				}
				// must be an lower boundary

				// shrink range above to single value range
				_, err := c.rdb.HSet(hitCutBelow.ID, "high", true).Result()
				if err != nil {
					return err
				}

				// remove cut, as there is no cutting of the range above
				// needed anymore.
				newRange = []*ipAttributes{
					singleBoundary,
				}
			}

			return c.insertBoundaries(newRange)
		}

	} else if closestBelow.LowerBound && closestAbove.UpperBound &&
		closestBelow.IsSingleBoundary() && closestAbove.IsSingleBoundary() {
		// inside a range
		ip, err := int64ToIP(singleInt - 1)
		if err != nil {
			return err
		}

		cutBelow := &ipAttributes{
			IP:         ip,
			Reason:     closestBelow.Reason,
			UpperBound: true,
		}

		ip, err = int64ToIP(singleInt + 1)
		if err != nil {
			return err
		}

		cutAbove := &ipAttributes{
			IP:         ip,
			Reason:     closestAbove.Reason,
			LowerBound: true,
		}

		// default case
		newRange := []*ipAttributes{
			cutBelow,
			singleBoundary,
			cutAbove,
		}

		boundaries, err := c.fetchBoundaries(cutBelow.IP, cutAbove.IP)
		if err != nil {
			return err
		}

		hitCutBelow, hitCutAbove := (*ipAttributes)(nil), (*ipAttributes)(nil)
		if len(boundaries) == 2 {
			hitCutBelow, hitCutAbove = boundaries[0], boundaries[1]
		}

		if hitCutBelow != nil && hitCutAbove != nil {
			tx := c.rdb.TxPipeline()

			// cutting above single value range
			// lower bound gets high attribute
			c.rdb.HSet(hitCutBelow.ID, "high", true)

			// cutting below single value range
			// upper bound gets low attribute
			c.rdb.HSet(hitCutAbove.ID, "low", true)

			_, err = tx.Exec()
			if err != nil {
				return err
			}

			// no cutting needed
			newRange = []*ipAttributes{
				singleBoundary,
			}

		} else if hitCutBelow != nil {
			// only hitCutBelow

			// boundary below becomes a single value range
			_, err = c.rdb.HSet(hitCutBelow.ID, "high", true).Result()
			if err != nil {
				return err
			}

			// only cutting above needed
			newRange = []*ipAttributes{
				singleBoundary,
				cutAbove,
			}

		} else if hitCutAbove != nil {
			// only hitCutAbove

			// boundary above becomes a single value range
			_, err = c.rdb.HSet(hitCutAbove.ID, "low", true).Result()
			if err != nil {
				return err
			}

			// only cutting below needed
			newRange = []*ipAttributes{
				cutBelow,
				singleBoundary,
			}
		}

		return c.insertBoundaries(newRange)
	}

	// not on boundary or inside a range
	newRange := []*ipAttributes{singleBoundary}

	return c.insertBoundaries(newRange)
}

// insertRangeInt properly inserts new ranges into the database, removing other ranges, cutting them, shrinking them, etc.
func (c *Client) insertRangeInt(lowInt64, highInt64 int64, reason string) error {

	inside, err := c.insideIntRange(lowInt64, highInt64)
	if err != nil {
		return err
	}

	belowLowerBound, aboveUpperBound, err := c.belowLowerAboveUpper(lowInt64, highInt64, 2)
	if err != nil {
		return err
	}

	// todo check if this lies outside of the range
	belowLowerClosest := belowLowerBound[len(belowLowerBound)-1]
	aboveUpperClosest := aboveUpperBound[0]

	ip, err := int64ToIP(lowInt64 - 1)
	if err != nil {
		return err
	}

	// todo: move cuts to their respective positions
	cutBelow := &ipAttributes{
		IP:         ip,
		Reason:     belowLowerClosest.Reason,
		UpperBound: true,
	}

	ip, err = int64ToIP(lowInt64)
	if err != nil {
		return err
	}

	lowerBound := &ipAttributes{
		IP:         ip,
		Reason:     reason,
		LowerBound: true,
	}

	ip, err = int64ToIP(highInt64)
	if err != nil {
		return err
	}

	upperBound := &ipAttributes{
		IP:         ip,
		Reason:     reason,
		UpperBound: true,
	}

	ip, err = int64ToIP(highInt64 + 1)
	if err != nil {
		return err
	}

	cutAbove := &ipAttributes{
		IP:         ip,
		Reason:     aboveUpperClosest.Reason,
		LowerBound: true,
	}

	// if cutAbove.EqualIP(aboveUpperClosest) {
	// 	runtime.Breakpoint()
	// }
	lenInside := len(inside)

	if lenInside == 0 {
		// nothin inside range

		var newRangeBoundaries []*ipAttributes

		if belowLowerClosest.LowerBound && aboveUpperClosest.UpperBound &&
			belowLowerClosest.IsSingleBoundary() && aboveUpperClosest.IsSingleBoundary() {
			// our new range is within a bigger range
			// len(inside) == 0 => outside range is connected

			// default case
			newRangeBoundaries = []*ipAttributes{
				cutBelow,
				lowerBound,
				upperBound,
				cutAbove,
			}

			boundaries, err := c.fetchBoundaries(cutBelow.IP, cutAbove.IP)
			if err != nil {
				return err
			}

			hitCutBelow, hitCutAbove := (*ipAttributes)(nil), (*ipAttributes)(nil)

			if len(boundaries) == 2 {
				hitCutBelow, hitCutAbove = boundaries[0], boundaries[1]
			}

			if hitCutBelow != nil && hitCutAbove != nil {
				// hit lower & upper boundary
				tx := c.rdb.TxPipeline()
				tx.HSet(hitCutBelow.ID, "high", true)
				tx.HSet(hitCutAbove.ID, "low", true)

				_, err = tx.Exec()
				if err != nil {
					return err
				}

				// hit both boundaries, insert only new range
				newRangeBoundaries = []*ipAttributes{
					lowerBound,
					upperBound,
				}

			} else if hitCutBelow != nil {
				// only hit lower boundary
				_, err = c.rdb.HSet(hitCutBelow.ID, "high", true).Result()
				if err != nil {
					return err
				}

				// insert everything except lower cut
				newRangeBoundaries = []*ipAttributes{
					lowerBound,
					upperBound,
					cutAbove,
				}
			} else if hitCutAbove != nil {
				// only hit upper boundary
				_, err = c.rdb.HSet(hitCutAbove.ID, "low", true).Result()
				if err != nil {
					return err
				}

				// insert everything except upper cut
				newRangeBoundaries = []*ipAttributes{
					cutBelow,
					lowerBound,
					upperBound,
				}
			}

		} else {
			//belowLowerClosest.UpperBound && aboveUpperClosest.LowerBound {
			// case 1: set empty, infinite boundaries guarantee
			// the existence of at least one neighbour to each side
			// case 2: inserting into empty space in between other ranges
			// belowLowerClosest != aboveUpperClosest, because len(inside) == 0
			// case 3: -inf below & other range above OR other range below & +inf above

			newRangeBoundaries = []*ipAttributes{
				lowerBound,
				upperBound,
			}
		}

		// sets boundaries below and above out to be inserted range
		return c.insertBoundaries(newRangeBoundaries)
	}

	// lenInside > 0

	insideMostLeft := inside[0]
	insideMostRight := inside[lenInside-1]

	if lenInside%2 == 0 {

		// default case, cut two ranges
		newRange := []*ipAttributes{
			cutBelow,
			lowerBound,
			upperBound,
			cutAbove,
		}

		// even number of boundaries inside of the range

		if insideMostLeft.LowerBound && insideMostRight.UpperBound {
			// all ranges are inside of the new range.
			// meaning they are smaller and can be replaced by the new bigger range

			newRange = []*ipAttributes{
				lowerBound,
				upperBound,
			}

		} else if insideMostLeft.UpperBound && insideMostRight.LowerBound &&
			insideMostLeft.IsSingleBoundary() && insideMostRight.IsSingleBoundary() {

			boundaries, err := c.fetchBoundaries(cutBelow.IP, cutAbove.IP)
			if err != nil {
				return err
			}

			hitCutBelow, hitCutAbove := (*ipAttributes)(nil), (*ipAttributes)(nil)

			if len(boundaries) == 2 {
				hitCutBelow, hitCutAbove = boundaries[0], boundaries[1]
			}

			if hitCutBelow != nil && hitCutAbove != nil {
				tx := c.rdb.TxPipeline()

				// cutting above single value range
				// lower bound gets high attribute
				c.rdb.HSet(hitCutBelow.ID, "high", true)

				// cutting below single value range
				// upper bound gets low attribute
				c.rdb.HSet(hitCutAbove.ID, "low", true)

				_, err = tx.Exec()
				if err != nil {
					return err
				}

				// no cutting needed
				newRange = []*ipAttributes{
					lowerBound,
					upperBound,
				}

			} else if hitCutBelow != nil {
				// only hitCutBelow

				// boundary below becomes a single value range
				_, err = c.rdb.HSet(hitCutBelow.ID, "high", true).Result()
				if err != nil {
					return err
				}

				// only cutting above needed
				newRange = []*ipAttributes{
					lowerBound,
					upperBound,
					cutAbove,
				}

			} else if hitCutAbove != nil {
				// only hitCutAbove

				// boundary above becomes a single value range
				_, err = c.rdb.HSet(hitCutAbove.ID, "low", true).Result()
				if err != nil {
					return err
				}

				// only cutting below needed
				newRange = []*ipAttributes{
					cutBelow,
					lowerBound,
					upperBound,
				}
			}

			// default value from above is used instead

		} else if insideMostLeft.UpperBound && insideMostLeft.IsSingleBoundary() {

			// default: nothing below the lower bound is hit when cutting
			newRange = []*ipAttributes{
				cutBelow,
				lowerBound,
				upperBound,
			}

			boundaries, err := c.fetchBoundaries(cutBelow.IP)
			if err != nil {
				return err
			}

			hitCutBelow := (*ipAttributes)(nil)
			if len(boundaries) == 1 {
				hitCutBelow = boundaries[0]

			}

			if hitCutBelow != nil {
				// only hitCutBelow

				// boundary below becomes a single value range
				_, err = c.rdb.HSet(hitCutBelow.ID, "high", true).Result()
				if err != nil {
					return err
				}

				// no cutting needed
				newRange = []*ipAttributes{
					lowerBound,
					upperBound,
				}
			}
		} else {
			// insideMostRight.LowerBound && insideMostRight.IsSingleBoundary()

			// default: nothing below the lower bound is hit when cutting
			newRange = []*ipAttributes{
				lowerBound,
				upperBound,
				cutAbove,
			}

			boundaries, err := c.fetchBoundaries(cutAbove.IP)
			if err != nil {
				return err
			}

			hitCutAbove := (*ipAttributes)(nil)

			if len(boundaries) == 1 {
				hitCutAbove = boundaries[0]
			}

			if hitCutAbove != nil {
				// only hitCutAbove

				// boundary below becomes a single value range
				_, err = c.rdb.HSet(hitCutAbove.ID, "low", true).Result()
				if err != nil {
					return err
				}

				// no cutting needed
				newRange = []*ipAttributes{
					lowerBound,
					upperBound,
				}
			}
		}

		// delete all inside boundaries
		err = c.removeIDs(idsOf(inside)...)
		if err != nil {
			return err
		}

		// insert lower cut, new range, upper cut boundary
		// depending on what is actually in the newRange
		return c.insertBoundaries(newRange)
	}

	// lenInside % 2 == 1
	// odd number of ranges inside the new range

	// delete all boundaries inside
	// of the new to be inserted range
	err = c.removeIDs(idsOf(inside)...)
	if err != nil {
		return err
	}

	var newRangeBoundaries []*ipAttributes

	if insideMostLeft.LowerBound && insideMostRight.UpperBound {
		// insideMostLeft.LowerBound && insideMostRight.UpperBound
		// nothing to cut, everything lies inside of the range
		newRangeBoundaries = []*ipAttributes{
			lowerBound,
			upperBound,
		}
	} else if insideMostLeft.UpperBound && insideMostRight.LowerBound &&
		insideMostLeft.IsSingleBoundary() && insideMostRight.IsSingleBoundary() {
		newRangeBoundaries = []*ipAttributes{
			cutBelow,
			lowerBound,
			upperBound,
			cutAbove,
		}

		boundaries, err := c.fetchBoundaries(cutBelow.IP, cutAbove.IP)
		if err != nil {
			return err
		}

		hitCutBelow, hitCutAbove := (*ipAttributes)(nil), (*ipAttributes)(nil)

		if len(boundaries) == 2 {
			hitCutBelow, hitCutAbove = boundaries[0], boundaries[1]
		}

		if hitCutBelow != nil && hitCutAbove != nil {
			// hit lower & upper boundary
			tx := c.rdb.TxPipeline()
			tx.HSet(hitCutBelow.ID, "high", true)
			tx.HSet(hitCutAbove.ID, "low", true)

			_, err = tx.Exec()
			if err != nil {
				return err
			}

			// hit both boundaries, insert only new range
			newRangeBoundaries = []*ipAttributes{
				lowerBound,
				upperBound,
			}

		} else if hitCutBelow != nil {
			// only hit lower boundary
			_, err = c.rdb.HSet(hitCutBelow.ID, "high", true).Result()
			if err != nil {
				return err
			}

			// insert everything except lower cut
			newRangeBoundaries = []*ipAttributes{
				lowerBound,
				upperBound,
				cutAbove,
			}
		} else if hitCutAbove != nil {
			// only hit upper boundary
			_, err = c.rdb.HSet(hitCutAbove.ID, "low", true).Result()
			if err != nil {
				return err
			}

			// insert everything except upper cut
			newRangeBoundaries = []*ipAttributes{
				cutBelow,
				lowerBound,
				upperBound,
			}
		}

	} else if insideMostLeft.UpperBound && insideMostLeft.IsSingleBoundary() {
		// the range at the lower end of the new range is partially
		// inside and partially outside the new range

		// default case if not hit anything while cutting
		newRangeBoundaries = []*ipAttributes{
			cutBelow,
			lowerBound,
			upperBound,
		}

		boundaries, err := c.fetchBoundaries(cutBelow.IP)
		if err != nil {
			return err
		}

		hitCutBelow := (*ipAttributes)(nil)

		if len(boundaries) == 1 {
			hitCutBelow = boundaries[0]
		}

		if hitCutBelow != nil {
			// only hit lower boundary
			_, err = c.rdb.HSet(hitCutBelow.ID, "high", true).Result()
			if err != nil {
				return err
			}

			// only insert new range boundaries
			newRangeBoundaries = []*ipAttributes{
				lowerBound,
				upperBound,
			}
		}

	} else {
		// insideMostRight.LowerBound && insideMostRight.IsSingleBoundary()

		// the range at the upper end of the new range that is to be inserted
		// is partially inside and partially outside the new range

		// default case that we do not hit anything when cutting above our new range
		newRangeBoundaries = []*ipAttributes{
			lowerBound,
			upperBound,
			cutAbove,
		}

		boundaries, err := c.fetchBoundaries(cutAbove.IP)
		if err != nil {
			return err
		}

		hitCutAbove := (*ipAttributes)(nil)

		if len(boundaries) == 1 {
			hitCutAbove = boundaries[0]
		}

		if hitCutAbove != nil {
			// only hit boundary above upper boundary
			_, err = c.rdb.HSet(hitCutAbove.ID, "low", true).Result()
			if err != nil {
				return err
			}

			// only insert new range boundaries
			newRangeBoundaries = []*ipAttributes{
				lowerBound,
				upperBound,
			}
		}
	}

	return c.insertBoundaries(newRangeBoundaries)
}

// Remove removes a range from the set
func (c *Client) Remove(ipRange string) error {
	low, high, err := boundaries(ipRange)

	if err != nil {
		return err
	}

	lowInt64, _ := ipToInt64(low)
	highInt64, _ := ipToInt64(high)

	if lowInt64 == highInt64 {
		// edge case, range is single value
		err = c.insertSingleInt(lowInt64, DeleteReason)
		if err != nil {
			return err
		}
	}

	err = c.insertRangeInt(lowInt64, highInt64, DeleteReason)
	if err != nil {
		return err
	}

	// get IDs of boundaries within given range
	boundaryIDs, err := c.insideIntIDs(lowInt64, highInt64)

	if err != nil {
		return err
	}

	// remove from sorted set and from attribute map
	return c.removeIDs(boundaryIDs...)
}

// insideIntIDs returns a list of range boundary IDs that lie within lowInt64 through highInt64.
// including these two boundaries.
func (c *Client) insideIntIDs(lowInt64, highInt64 int64) ([]string, error) {
	tx := c.rdb.TxPipeline()

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

	// force sorting
	sort.Sort(byScore(insideResults))

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
func (c *Client) insideIntRange(lowInt64, highInt64 int64) (inside []*ipAttributes, err error) {
	inside = make([]*ipAttributes, 0, 3)

	tx := c.rdb.TxPipeline()

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
		attr, err := c.fetchIPAttributes(result)
		if err != nil {
			return nil, err
		}

		inside = append(inside, attr)
	}

	sort.Sort(byAttributeIP(inside))
	return
}

// insideInfRange returns all ranges
func (c *Client) insideInfRange() (inside []*ipAttributes, err error) {
	inside = make([]*ipAttributes, 0, 3)

	tx := c.rdb.TxPipeline()

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
		attr, err := c.fetchIPAttributes(result)
		if err != nil {
			return nil, err
		}

		inside = append(inside, attr)
	}

	sort.Sort(byAttributeIP(inside))
	return
}

func (c *Client) belowLowerAboveUpper(lower, upper, num int64) (belowLower, aboveUpper []*ipAttributes, err error) {

	tx := c.rdb.TxPipeline()

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

	belowLower, err = c.fetchAllIPAttributes(belowLowerResults...)
	if err != nil {
		return nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	aboveUpper, err = c.fetchAllIPAttributes(aboveUpperResults...)
	if err != nil {
		return nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	return
}

// neighboursInt does not do any checks, thus making it reusable in other methods without check overhead
func (c *Client) neighboursInt(ofIP int64, numNeighbours uint) (below, above []*ipAttributes, err error) {

	below = make([]*ipAttributes, 0, numNeighbours)
	above = make([]*ipAttributes, 0, numNeighbours)

	tx := c.rdb.TxPipeline()

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
		attr, err := c.fetchIPAttributes(result)
		if err != nil {
			return nil, nil, err
		}

		// prepend for correct order
		below = append([]*ipAttributes{attr}, below...)
	}

	aboveResults, err := cmdAbove.Result()

	if err != nil {
		return nil, nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	for _, result := range aboveResults {
		attr, err := c.fetchIPAttributes(result)
		if err != nil {
			return nil, nil, err
		}

		above = append(above, attr)
	}

	return
}

// fetchIpAttributes gets the remaining IP related attributes that belong to the IP range boundary
// that is encoded in the redis.Z.Score attribute
func (c *Client) fetchIPAttributes(result redis.Z) (*ipAttributes, error) {

	switch result.Score {
	case math.Inf(-1):
		return globalLowerBoundary, nil
	case math.Inf(1):
		return globalUpperBoundary, nil
	}

	id := ""
	var resultIP net.IP
	var err error

	switch t := result.Member.(type) {
	case string:
		id = t
		resultIP, err = float64ToIP(result.Score)
		if err != nil {
			return nil, err
		}

	default:
		return nil, fmt.Errorf("%w : member result is not of type string : %T", ErrNoResult, t)
	}

	fields, err := c.rdb.HMGet(id, "low", "high", "reason").Result()

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

	return &ipAttributes{
		ID:         id,
		IP:         resultIP,
		Reason:     reason,
		LowerBound: low,
		UpperBound: high,
	}, nil
}

// fetch a list of ipAttributes passed as result parameters
func (c *Client) fetchAllIPAttributes(results ...redis.Z) ([]*ipAttributes, error) {

	attributes := make([]*ipAttributes, 0, len(results))

	for _, result := range results {
		switch result.Score {
		case math.Inf(-1):
			attributes = append(attributes, globalLowerBoundary)
			continue
		case math.Inf(1):
			attributes = append(attributes, globalUpperBoundary)
			continue
		}

		id := ""
		var resultIP net.IP
		var err error

		switch t := result.Member.(type) {
		case string:
			id = t
			resultIP, err = float64ToIP(result.Score)
			if err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("%w : member result is not of type string : %T", ErrNoResult, t)
		}

		fields, err := c.rdb.HMGet(id, "low", "high", "reason").Result()

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

		attributes = append(attributes, &ipAttributes{
			ID:         id,
			IP:         resultIP,
			Reason:     reason,
			LowerBound: low,
			UpperBound: high,
		})
	}

	return attributes, nil
}

func (c *Client) fetchBoundaries(ips ...net.IP) ([]*ipAttributes, error) {
	intIPs := make([]int64, len(ips))
	for idx, ip := range ips {

		aIP, err := ipToInt64(ip)
		if err != nil {
			return nil, err
		}

		intIPs[idx] = aIP
	}

	tx := c.rdb.Pipeline()

	cmds := make([]*redis.StringSliceCmd, 0, len(ips))

	for _, intIP := range intIPs {
		cmd := tx.ZRangeByScore(IPRangesKey, redis.ZRangeBy{
			Min: strconv.FormatInt(intIP, 10),
			Max: strconv.FormatInt(intIP, 10),
		})

		cmds = append(cmds, cmd)
	}

	_, err := tx.Exec()
	if err != nil {
		return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
	}

	result := make([]*ipAttributes, 0, len(ips))

	for idx, cmd := range cmds {
		boundaries, err := cmd.Result()
		if err != nil {
			return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		if len(boundaries) == 0 {
			result = append(result, nil)
		} else {
			id := boundaries[0]

			attr, err := c.rdb.HMGet(id, "reason", "low", "high").Result()
			if err != nil {
				return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
			}
			if len(attr) < 3 {
				result = append(result, nil)
				continue
			}

			reason := ""

			if t, ok := attr[0].(string); ok {
				reason = t
			}

			low := false
			switch t := attr[1].(type) {
			case string:
				low = t != "0"
			case bool:
				low = t
			}

			high := false
			switch t := attr[2].(type) {
			case string:
				high = t != "0"
			case bool:
				high = t
			}

			if !low && low == high {
				result = append(result, nil)
				continue
			}

			resultAttr := &ipAttributes{
				ID:         id,
				IP:         ips[idx],
				Reason:     reason,
				LowerBound: low,
				UpperBound: high,
			}
			result = append(result, resultAttr)
		}
	}

	return result, nil
}

func (c *Client) removeIDs(ids ...string) error {
	tx := c.rdb.TxPipeline()

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
func idsOf(attributes []*ipAttributes) []string {
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

// removes dubplicates that are next to each other.
func ipsOf(attributes []*ipAttributes) []net.IP {
	ids := make([]net.IP, 0, len(attributes))

	for idx, attr := range attributes {
		// attributes are sorted
		if idx > 0 && len(attr.ID) > 0 && attr.EqualIP(attributes[idx-1]) {
			// skip continuous duplicates
			continue
		}

		if attr.IP != nil {
			ids = append(ids, attr.IP)
		}

	}
	return ids
}

// Find returns a non empty reason and nil for an error if the given IP is
// found within any previously inserted IP range.
// An error is returned if the request fails and thus should be treated as false.
func (c *Client) Find(ip string) (string, error) {
	reqIP, _, err := boundaries(ip)

	if err != nil {
		return "", err
	}

	intIP, _ := ipToInt64(reqIP)

	belowN, aboveN, err := c.neighboursInt(intIP, 1)
	if err != nil {
		return "", err
	}

	// this is enforced by the idempotent database initialization.
	below, above := belowN[0], aboveN[0]

	if below.Equal(above) && below.IP.Equal(reqIP) {
		return below.Reason, nil
	}

	inRange := below.LowerBound && !below.UpperBound && !above.LowerBound && above.UpperBound

	if inRange && below.Reason != above.Reason {
		panic(fmt.Errorf(" '%s'.Reason != '%s'.Reason : '%s' != '%s'", below.ID, above.ID, below.Reason, above.Reason))
	}

	if !inRange {
		return "", ErrIPNotFound
	}

	if below.Reason == DeleteReason {
		return "", ErrNoResult
	}

	return below.Reason, nil
}
