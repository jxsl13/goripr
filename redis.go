package goripr

import (
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/go-redis/redis"
)

// Client is an extended version of the redis.Client
type Client struct {
	*redis.Client
}

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
	return &Client{rdb}, nil

}

// insertBoundaries does not do any range checks, allowing for a little bit more performance
// Side effect: if boundary.ID == "" -> it gets a new UUID
func (rdb *Client) insertBoundaries(boundaries []*IPAttributes) error {

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

		bigIntIP, ipBits := IPToInt(boundary.IP)

		if ipBits > IPv4Bits {
			return ErrIPv6NotSupported
		}

		intIP := bigIntIP.Int64()

		// insert into sorted set
		tx.ZAdd(IPRangesKey,
			redis.Z{
				Score:  float64(intIP),
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
func (rdb *Client) Insert(ipRange, reason string) error {
	low, high, err := Boundaries(ipRange)

	if err != nil {
		return err
	}

	lowInt, _ := IPToInt(low)
	highInt, _ := IPToInt(high)

	lowInt64 := lowInt.Int64()
	highInt64 := highInt.Int64()

	if lowInt64 == highInt64 {
		// edge case, range is single value
		return rdb.insertSingleInt(lowInt64, reason)
	}

	return rdb.insertRangeInt(lowInt64, highInt64, reason)
}

func (rdb *Client) insertSingleInt(singleInt int64, reason string) error {

	below, above, err := rdb.neighboursInt(singleInt, 1)
	if err != nil {
		return err
	}

	closestBelow := below[len(below)-1]
	closestAbove := above[0]

	singleBoundary := &IPAttributes{
		IP:         IntToIP(big.NewInt(singleInt), IPv4Bits),
		Reason:     reason,
		LowerBound: true,
		UpperBound: true,
	}

	if closestBelow.Equal(closestAbove) {

		// hittig an edge / a boundary directly
		hitBoundary := closestBelow

		// remove the hit boundary
		err := rdb.removeIDs(hitBoundary.ID)
		if err != nil {
			return err
		}

		if !hitBoundary.IsSingleBoundary() {
			// hit a single value range
			// simply replace it

			newRange := []*IPAttributes{
				singleBoundary,
			}

			return rdb.insertBoundaries(newRange)

		} else if hitBoundary.LowerBound {
			// must be single boundary, meaning a range with at least two members

			cutAbove := &IPAttributes{
				IP:         IntToIP(big.NewInt(hitBoundary.IPInt64()+1), IPv4Bits),
				Reason:     hitBoundary.Reason,
				LowerBound: true,
			}

			// default case
			newRange := []*IPAttributes{
				singleBoundary,
				cutAbove,
			}

			boundaries, err := rdb.fetchBoundaries(cutAbove.IP)
			if err != nil {
				return err
			}

			hitCutAbove := (*IPAttributes)(nil)
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
				_, err := rdb.HSet(hitCutAbove.ID, "low", true).Result()
				if err != nil {
					return err
				}

				// remove cut, as there is no cutting of the range above
				// needed anymore.
				newRange = []*IPAttributes{
					singleBoundary,
				}
			}

			return rdb.insertBoundaries(newRange)

		} else {
			// hitBoundary.UpperBound

			cutBelow := &IPAttributes{
				IP:         IntToIP(big.NewInt(hitBoundary.IPInt64()-1), IPv4Bits),
				Reason:     hitBoundary.Reason,
				UpperBound: true,
			}

			boundaries, err := rdb.fetchBoundaries(cutBelow.IP)
			if err != nil {
				return err
			}

			hitCutBelow := (*IPAttributes)(nil)
			if len(boundaries) == 1 {
				hitCutBelow = boundaries[0]
			}

			// default case
			newRange := []*IPAttributes{
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
				_, err := rdb.HSet(hitCutBelow.ID, "high", true).Result()
				if err != nil {
					return err
				}

				// remove cut, as there is no cutting of the range above
				// needed anymore.
				newRange = []*IPAttributes{
					singleBoundary,
				}
			}

			return rdb.insertBoundaries(newRange)
		}

	} else if closestBelow.LowerBound && closestAbove.UpperBound &&
		closestBelow.IsSingleBoundary() && closestAbove.IsSingleBoundary() {
		// inside a range
		// TODO: try not to hit upper or lower boundaries when cutting above or below the current one
		cutBelow := &IPAttributes{
			IP:         IntToIP(big.NewInt(singleInt-1), IPv4Bits),
			Reason:     closestBelow.Reason,
			UpperBound: true,
		}
		cutAbove := &IPAttributes{
			IP:         IntToIP(big.NewInt(singleInt+1), IPv4Bits),
			Reason:     closestAbove.Reason,
			LowerBound: true,
		}

		// default case
		newRange := []*IPAttributes{
			cutBelow,
			singleBoundary,
			cutAbove,
		}

		boundaries, err := rdb.fetchBoundaries(cutBelow.IP, cutAbove.IP)
		if err != nil {
			return err
		}

		hitCutBelow, hitCutAbove := (*IPAttributes)(nil), (*IPAttributes)(nil)
		if len(boundaries) == 2 {
			hitCutBelow, hitCutAbove = boundaries[0], boundaries[1]
		}

		if hitCutBelow != nil && hitCutAbove != nil {
			tx := rdb.TxPipeline()

			// cutting above single value range
			// lower bound gets high attribute
			rdb.HSet(hitCutBelow.ID, "high", true)

			// cutting below single value range
			// upper bound gets low attribute
			rdb.HSet(hitCutAbove.ID, "low", true)

			_, err = tx.Exec()
			if err != nil {
				return err
			}

			// no cutting needed
			newRange = []*IPAttributes{
				singleBoundary,
			}

		} else if hitCutBelow != nil {
			// only hitCutBelow

			// boundary below becomes a single value range
			_, err = rdb.HSet(hitCutBelow.ID, "high", true).Result()
			if err != nil {
				return err
			}

			// only cutting above needed
			newRange = []*IPAttributes{
				singleBoundary,
				cutAbove,
			}

		} else if hitCutAbove != nil {
			// only hitCutAbove

			// boundary above becomes a single value range
			_, err = rdb.HSet(hitCutAbove.ID, "low", true).Result()
			if err != nil {
				return err
			}

			// only cutting below needed
			newRange = []*IPAttributes{
				cutBelow,
				singleBoundary,
			}
		}

		return rdb.insertBoundaries(newRange)
	}

	// not on boundary or inside a range
	newRange := []*IPAttributes{singleBoundary}

	return rdb.insertBoundaries(newRange)
}

// insertRangeInt properly inserts new ranges into the database, removing other ranges, cutting them, shrinking them, etc.
func (rdb *Client) insertRangeInt(lowInt64, highInt64 int64, reason string) error {

	inside, err := rdb.insideIntRange(lowInt64, highInt64)
	if err != nil {
		return err
	}

	belowLowerBound, aboveUpperBound, err := rdb.belowLowerAboveUpper(lowInt64, highInt64, 2)
	if err != nil {
		return err
	}

	// todo check if this lies outside of the range
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

		newRangeBoundaries := []*IPAttributes{}

		if belowLowerClosest.LowerBound && aboveUpperClosest.UpperBound &&
			belowLowerClosest.IsSingleBoundary() && aboveUpperClosest.IsSingleBoundary() {
			// our new range is within a bigger range
			// len(inside) == 0 => outside range is connected

			// default case
			newRangeBoundaries = []*IPAttributes{
				cutBelow,
				lowerBound,
				upperBound,
				cutAbove,
			}

			boundaries, err := rdb.fetchBoundaries(cutBelow.IP, cutAbove.IP)
			if err != nil {
				return err
			}

			hitCutBelow, hitCutAbove := (*IPAttributes)(nil), (*IPAttributes)(nil)

			if len(boundaries) == 2 {
				hitCutBelow, hitCutAbove = boundaries[0], boundaries[1]
			}

			if hitCutBelow != nil && hitCutAbove != nil {
				// hit lower & upper boundary
				tx := rdb.TxPipeline()
				tx.HSet(hitCutBelow.ID, "high", true)
				tx.HSet(hitCutAbove.ID, "low", true)

				_, err = tx.Exec()
				if err != nil {
					return err
				}

				// hit both boundaries, insert only new range
				newRangeBoundaries = []*IPAttributes{
					lowerBound,
					upperBound,
				}

			} else if hitCutBelow != nil {
				// only hit lower boundary
				_, err = rdb.HSet(hitCutBelow.ID, "high", true).Result()
				if err != nil {
					return err
				}

				// insert everything except lower cut
				newRangeBoundaries = []*IPAttributes{
					lowerBound,
					upperBound,
					cutAbove,
				}
			} else if hitCutAbove != nil {
				// only hit upper boundary
				_, err = rdb.HSet(hitCutAbove.ID, "low", true).Result()
				if err != nil {
					return err
				}

				// insert everything except upper cut
				newRangeBoundaries = []*IPAttributes{
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

			newRangeBoundaries = []*IPAttributes{
				lowerBound,
				upperBound,
			}
		}

		// sets boundaries below and above out to be inserted range
		return rdb.insertBoundaries(newRangeBoundaries)
	}

	// lenInside > 0

	insideMostLeft := inside[0]
	insideMostRight := inside[lenInside-1]

	if lenInside%2 == 0 {

		// default case, cut two ranges
		newRange := []*IPAttributes{
			cutBelow,
			lowerBound,
			upperBound,
			cutAbove,
		}

		// even number of boundaries inside of the range

		if insideMostLeft.LowerBound && insideMostRight.UpperBound {
			// all ranges are inside of the new range.
			// meaning they are smaller and can be replaced by the new bigger range

			newRange = []*IPAttributes{
				lowerBound,
				upperBound,
			}

		} else if insideMostLeft.UpperBound && insideMostRight.LowerBound &&
			insideMostLeft.IsSingleBoundary() && insideMostRight.IsSingleBoundary() {

			boundaries, err := rdb.fetchBoundaries(cutBelow.IP, cutAbove.IP)
			if err != nil {
				return err
			}

			hitCutBelow, hitCutAbove := (*IPAttributes)(nil), (*IPAttributes)(nil)

			if len(boundaries) == 2 {
				hitCutBelow, hitCutAbove = boundaries[0], boundaries[1]
			}

			if hitCutBelow != nil && hitCutAbove != nil {
				tx := rdb.TxPipeline()

				// cutting above single value range
				// lower bound gets high attribute
				rdb.HSet(hitCutBelow.ID, "high", true)

				// cutting below single value range
				// upper bound gets low attribute
				rdb.HSet(hitCutAbove.ID, "low", true)

				_, err = tx.Exec()
				if err != nil {
					return err
				}

				// no cutting needed
				newRange = []*IPAttributes{
					lowerBound,
					upperBound,
				}

			} else if hitCutBelow != nil {
				// only hitCutBelow

				// boundary below becomes a single value range
				_, err = rdb.HSet(hitCutBelow.ID, "high", true).Result()
				if err != nil {
					return err
				}

				// only cutting above needed
				newRange = []*IPAttributes{
					lowerBound,
					upperBound,
					cutAbove,
				}

			} else if hitCutAbove != nil {
				// only hitCutAbove

				// boundary above becomes a single value range
				_, err = rdb.HSet(hitCutAbove.ID, "low", true).Result()
				if err != nil {
					return err
				}

				// only cutting below needed
				newRange = []*IPAttributes{
					cutBelow,
					lowerBound,
					upperBound,
				}
			}

			// default value from above is used instead

		} else if insideMostLeft.UpperBound && insideMostLeft.IsSingleBoundary() {

			// default: nothing below the lower bound is hit when cutting
			newRange = []*IPAttributes{
				cutBelow,
				lowerBound,
				upperBound,
			}

			boundaries, err := rdb.fetchBoundaries(cutBelow.IP)
			if err != nil {
				return err
			}

			hitCutBelow := (*IPAttributes)(nil)
			if len(boundaries) == 1 {
				hitCutBelow = boundaries[0]

			}

			if hitCutBelow != nil {
				// only hitCutBelow

				// boundary below becomes a single value range
				_, err = rdb.HSet(hitCutBelow.ID, "high", true).Result()
				if err != nil {
					return err
				}

				// no cutting needed
				newRange = []*IPAttributes{
					lowerBound,
					upperBound,
				}
			}
		} else {
			// insideMostRight.LowerBound && insideMostRight.IsSingleBoundary()

			// default: nothing below the lower bound is hit when cutting
			newRange = []*IPAttributes{
				lowerBound,
				upperBound,
				cutAbove,
			}

			boundaries, err := rdb.fetchBoundaries(cutAbove.IP)
			if err != nil {
				return err
			}

			hitCutAbove := (*IPAttributes)(nil)

			if len(boundaries) == 1 {
				hitCutAbove = boundaries[0]
			}

			if hitCutAbove != nil {
				// only hitCutAbove

				// boundary below becomes a single value range
				_, err = rdb.HSet(hitCutAbove.ID, "low", true).Result()
				if err != nil {
					return err
				}

				// no cutting needed
				newRange = []*IPAttributes{
					lowerBound,
					upperBound,
				}
			}
		}

		// delete all inside boundaries
		err = rdb.removeIDs(idsOf(inside)...)
		if err != nil {
			return err
		}

		// insert lower cut, new range, upper cut boundary
		// depending on what is actually in the newRange
		return rdb.insertBoundaries(newRange)
	}

	// lenInside % 2 == 1
	// odd number of ranges inside the new range

	// delete all boundaries inside
	// of the new to be inserted range
	err = rdb.removeIDs(idsOf(inside)...)
	if err != nil {
		return err
	}

	newRangeBoundaries := []*IPAttributes{}

	if insideMostLeft.LowerBound && insideMostRight.UpperBound {
		// insideMostLeft.LowerBound && insideMostRight.UpperBound
		// nothing to cut, everything lies inside of the range
		newRangeBoundaries = []*IPAttributes{
			lowerBound,
			upperBound,
		}
	} else if insideMostLeft.UpperBound && insideMostRight.LowerBound &&
		insideMostLeft.IsSingleBoundary() && insideMostRight.IsSingleBoundary() {
		newRangeBoundaries = []*IPAttributes{
			cutBelow,
			lowerBound,
			upperBound,
			cutAbove,
		}

		boundaries, err := rdb.fetchBoundaries(cutBelow.IP, cutAbove.IP)
		if err != nil {
			return err
		}

		hitCutBelow, hitCutAbove := (*IPAttributes)(nil), (*IPAttributes)(nil)

		if len(boundaries) == 2 {
			hitCutBelow, hitCutAbove = boundaries[0], boundaries[1]
		}

		if hitCutBelow != nil && hitCutAbove != nil {
			// hit lower & upper boundary
			tx := rdb.TxPipeline()
			tx.HSet(hitCutBelow.ID, "high", true)
			tx.HSet(hitCutAbove.ID, "low", true)

			_, err = tx.Exec()
			if err != nil {
				return err
			}

			// hit both boundaries, insert only new range
			newRangeBoundaries = []*IPAttributes{
				lowerBound,
				upperBound,
			}

		} else if hitCutBelow != nil {
			// only hit lower boundary
			_, err = rdb.HSet(hitCutBelow.ID, "high", true).Result()
			if err != nil {
				return err
			}

			// insert everything except lower cut
			newRangeBoundaries = []*IPAttributes{
				lowerBound,
				upperBound,
				cutAbove,
			}
		} else if hitCutAbove != nil {
			// only hit upper boundary
			_, err = rdb.HSet(hitCutAbove.ID, "low", true).Result()
			if err != nil {
				return err
			}

			// insert everything except upper cut
			newRangeBoundaries = []*IPAttributes{
				cutBelow,
				lowerBound,
				upperBound,
			}
		}

	} else if insideMostLeft.UpperBound && insideMostLeft.IsSingleBoundary() {
		// the range at the lower end of the new range is partially
		// inside and partially outside the new range

		// default case if not hit anything while cutting
		newRangeBoundaries = []*IPAttributes{
			cutBelow,
			lowerBound,
			upperBound,
		}

		boundaries, err := rdb.fetchBoundaries(cutBelow.IP)
		if err != nil {
			return err
		}

		hitCutBelow := (*IPAttributes)(nil)

		if len(boundaries) == 1 {
			hitCutBelow = boundaries[0]
		}

		if hitCutBelow != nil {
			// only hit lower boundary
			_, err = rdb.HSet(hitCutBelow.ID, "high", true).Result()
			if err != nil {
				return err
			}

			// only insert new range boundaries
			newRangeBoundaries = []*IPAttributes{
				lowerBound,
				upperBound,
			}
		}

	} else {
		// insideMostRight.LowerBound && insideMostRight.IsSingleBoundary()

		// the range at the upper end of the new range that is to be inserted
		// is partially inside and partially outside the new range

		// default case that we do not hit anything when cutting above our new range
		newRangeBoundaries = []*IPAttributes{
			lowerBound,
			upperBound,
			cutAbove,
		}

		boundaries, err := rdb.fetchBoundaries(cutAbove.IP)
		if err != nil {
			return err
		}

		hitCutAbove := (*IPAttributes)(nil)

		if len(boundaries) == 1 {
			hitCutAbove = boundaries[0]
		}

		if hitCutAbove != nil {
			// only hit boundary above upper boundary
			_, err = rdb.HSet(hitCutAbove.ID, "low", true).Result()
			if err != nil {
				return err
			}

			// only insert new range boundaries
			newRangeBoundaries = []*IPAttributes{
				lowerBound,
				upperBound,
			}
		}
	}

	return rdb.insertBoundaries(newRangeBoundaries)
}

// Remove removes a range from the set
func (rdb *Client) Remove(ipRange string) error {
	low, high, err := Boundaries(ipRange)

	if err != nil {
		return err
	}

	lowInt, _ := IPToInt(low)
	highInt, _ := IPToInt(high)

	lowInt64 := lowInt.Int64()
	highInt64 := highInt.Int64()

	if lowInt64 == highInt64 {
		// edge case, range is single value
		err = rdb.insertSingleInt(lowInt64, DeleteReason)
		if err != nil {
			return err
		}
	}

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
	return rdb.removeIDs(boundaryIDs...)
}

// insideIntIDs returns a list of range boundary IDs that lie within lowInt64 through highInt64.
// including these two boundaries.
func (rdb *Client) insideIntIDs(lowInt64, highInt64 int64) ([]string, error) {
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
func (rdb *Client) insideIntRange(lowInt64, highInt64 int64) (inside []*IPAttributes, err error) {
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

	sort.Sort(byAttributeIP(inside))
	return
}

// insideInfRange returns all ranges
func (rdb *Client) insideInfRange() (inside []*IPAttributes, err error) {
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

	sort.Sort(byAttributeIP(inside[1 : len(inside)-2]))
	return
}

func (rdb *Client) belowLowerAboveUpper(lower, upper, num int64) (belowLower, aboveUpper []*IPAttributes, err error) {

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

// neighboursInt does not do any checks, thus making it reusable in other methods without check overhead
func (rdb *Client) neighboursInt(ofIP int64, numNeighbours uint) (below, above []*IPAttributes, err error) {

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
func (rdb *Client) fetchIPAttributes(result redis.Z) (*IPAttributes, error) {

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
func (rdb *Client) fetchAllIPAttributes(results ...redis.Z) ([]*IPAttributes, error) {

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

func (rdb *Client) fetchBoundaries(ips ...net.IP) ([]*IPAttributes, error) {
	intIPs := make([]int64, 0, len(ips))
	for _, ip := range ips {
		bInt, _ := IPToInt(ip)
		intIPs = append(intIPs, bInt.Int64())
	}

	tx := rdb.Pipeline()

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

	result := make([]*IPAttributes, 0, len(ips))

	for idx, cmd := range cmds {
		boundaries, err := cmd.Result()
		if err != nil {
			return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
		}

		if len(boundaries) == 0 {
			result = append(result, nil)
		} else {
			id := boundaries[0]

			attr, err := rdb.HMGet(id, "reason", "low", "high").Result()
			if err != nil {
				return nil, fmt.Errorf("%w : %v", ErrNoResult, err)
			}
			if len(attr) < 3 {
				result = append(result, nil)
				continue
			}

			reason := ""
			switch t := attr[0].(type) {
			case string:
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

			if low == false && low == high {
				result = append(result, nil)
				continue
			}

			resultAttr := &IPAttributes{
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

func (rdb *Client) removeIDs(ids ...string) error {
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

// removes dubplicates that are next to each other.
func ipsOf(attributes []*IPAttributes) []net.IP {
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
func (rdb *Client) Find(ip string) (string, error) {
	reqIP, _, err := Boundaries(ip)

	if err != nil {
		return "", err
	}

	bigIntIP, _ := IPToInt(reqIP)

	intIP := bigIntIP.Int64()

	belowN, aboveN, err := rdb.neighboursInt(intIP, 1)
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

	if below.Reason == DeleteReason {
		return "", ErrNoResult
	}

	return below.Reason, nil
}
