package goripr

import (
	"context"
	"math"
	"net"
	"strconv"

	"github.com/redis/go-redis/v9"
	"github.com/xgfone/go-netaddr"
)

var (
	negInfBoundary = boundary{
		ID:         "-inf",
		IP:         nil,
		Int64:      int64(math.Inf(-1)),
		Float64:    math.Inf(-1),
		UpperBound: true,
		Reason:     "-inf",
	}

	posInfBoundary = boundary{
		ID:         "+inf",
		IP:         nil,
		Int64:      int64(math.Inf(1)),
		Float64:    math.Inf(1),
		LowerBound: true,
		Reason:     "+inf",
	}
)

type boundary struct {
	ID         string
	IP         net.IP
	Int64      int64
	Float64    float64
	LowerBound bool
	UpperBound bool
	Reason     string
}

func newBoundary(ip interface{}, reason string, lower, upper bool) boundary {

	var IP netaddr.IPAddress
	var err error
	var b boundary

	switch t := ip.(type) {
	case float32:
		IP, err = netaddr.NewIPAddress(int64(t))

	case float64:
		// if infinity boundaries are retrieved, simply return the global constants
		if t == math.Inf(-1) {
			return negInfBoundary
		} else if t == math.Inf(1) {
			return posInfBoundary
		}

		IP, err = netaddr.NewIPAddress(int64(t))
	case string:

		// string contains integer
		i, e := strconv.ParseInt(t, 10, 64)
		if e == nil {
			IP, err = netaddr.NewIPAddress(i)
			break
		}

		// string contains float
		f, e := strconv.ParseFloat(t, 64)
		if e == nil {
			IP, err = netaddr.NewIPAddress(int64(f))
			break
		}

		// string contains IP
		IP, err = netaddr.NewIPAddress(t)
	default:
		// string contains uint32, uint64, etc
		IP, err = netaddr.NewIPAddress(t, 4)
	}

	if err != nil {
		panic(err)
	}

	i64 := IP.BigInt().Int64()

	b = boundary{
		ID:         IP.String(),
		IP:         IP.IP(),
		Int64:      i64,
		Float64:    float64(i64),
		LowerBound: lower,
		UpperBound: upper,
		Reason:     reason,
	}
	return b
}

// Int64String returns the string representation of the Int64 value
func (b *boundary) Int64String() string {
	return strconv.FormatInt(b.Int64, 10)
}

func (b *boundary) Cmp(other boundary) int {
	if b.Int64 == other.Int64 {
		return 0
	} else if b.Int64 < other.Int64 {
		return -1
	}

	return 1
}

// Below returns a new boundary that is one IP below the current one.
// it does not set any of the two boundaries, thus needing them to be set manually!!
func (b *boundary) Below() boundary {
	below := newBoundary(b.Int64-1, b.Reason, false, false)

	return below
}

// Above returns a new boundary that is one IP above the current one.
// it does not set any of the two boundaries, thus needing them to be set manually!!
func (b *boundary) Above() boundary {
	above := newBoundary(b.Int64+1, b.Reason, false, false)

	return above
}

// IsSingleBoundary returns true if b is only one of both boundaries, either only lower or only upperbound
func (b *boundary) IsSingleBound() bool {
	return b.LowerBound != b.UpperBound
}

// SetLowerBound sets b to be a single lower boundary.
func (b *boundary) SetLowerBound() {
	b.LowerBound = true
	b.UpperBound = false
}

// IsLowerBound only returns true if the boundary is a single boundary as well as a lower boundary.
func (b *boundary) IsLowerBound() bool {
	return b.IsSingleBound() && b.LowerBound
}

// SetLowerBound sets b to be a single upper boundary.
func (b *boundary) SetUpperBound() {
	b.LowerBound = false
	b.UpperBound = true
}

// IsUpperBound only returns true if the boundary is a single boundary as well as an upper boundary.
func (b *boundary) IsUpperBound() bool {
	return b.IsSingleBound() && b.UpperBound
}

// SetDoubleBound sets b to be a lower as well as an upper boundary
func (b *boundary) SetDoubleBound() {
	b.LowerBound = true
	b.UpperBound = true
}

// IsDoubleBound only returns true if both lower and upper bounds are true
func (b *boundary) IsDoubleBound() bool {
	if !b.LowerBound && !b.UpperBound {
		panic("invalid boundary state")
	}
	return b.LowerBound && b.UpperBound
}

// Equal tests, whether both b and other have exactly the same members.
func (b *boundary) Equal(other boundary) bool {
	return b.ID == other.ID &&
		b.IP.Equal(other.IP) &&
		b.Int64 == other.Int64 &&
		b.Float64 == other.Float64 &&
		b.LowerBound == other.LowerBound &&
		b.UpperBound == other.UpperBound &&
		b.Reason == other.Reason
}

// EqualIP returns true if both IPs are equal as well as both Int64 and Float64 values.
func (b *boundary) EqualIP(other boundary) bool {
	return b.IP.Equal(other.IP) &&
		b.Int64 == other.Int64 &&
		b.Float64 == other.Float64
}

// EqualReason returns true if both reasons are equal, false otherwise.
func (b *boundary) HasReason() bool {
	return b.Reason != ""
}

// EqualReason returns true if both reasons are equal and not empty, false otherwise.
func (b *boundary) EqualReason(other boundary) bool {
	return b.HasReason() && other.HasReason() && b.Reason == other.Reason
}

// Insert adds the necessary commands to the transaction in order to be properly inserted.
func (b *boundary) Insert(ctx context.Context, tx redis.Pipeliner) redis.Pipeliner {
	tx.ZAdd(ctx, IPRangesKey,
		redis.Z{
			Score:  b.Float64,
			Member: b.ID,
		},
	)
	tx.HMSet(ctx, b.ID,
		map[string]interface{}{
			"low":    b.LowerBound,
			"high":   b.UpperBound,
			"reason": b.Reason,
		})
	return tx
}

// Update adds the needed commands to the transaction in order to update the assiciated attributes of the
// unserlying IP. The IP itself cannot be updated with this command.
func (b *boundary) Update(ctx context.Context, tx redis.Pipeliner) redis.Pipeliner {
	tx.HMSet(ctx, b.ID,
		map[string]interface{}{
			"low":    b.LowerBound,
			"high":   b.UpperBound,
			"reason": b.Reason,
		})
	return tx
}

// Remove adds the necessary commands to the transaction in order to be properly removed.
func (b *boundary) Remove(ctx context.Context, tx redis.Pipeliner) redis.Pipeliner {
	tx.ZRem(ctx, IPRangesKey, b.ID)
	tx.Del(ctx, b.ID)
	return tx
}

// Get adds the necessary commands to the transaction in order to retrieve the attributs from the database.
func (b *boundary) Get(ctx context.Context, tx redis.Pipeliner) *redis.SliceCmd {
	return tx.HMGet(ctx, b.ID, "low", "high", "reason")
}
