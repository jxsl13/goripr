package goripr

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
)

// Error is a wrapper for constant errors that are not supposed to be changed.
type Error string

func (e Error) Error() string { return string(e) }

// ipAttributes is the composite result type of anything requesting an IP
type ipAttributes struct {
	ID         string
	IP         net.IP
	Reason     string
	LowerBound bool
	UpperBound bool
}

// Equal tests if two attribute instances are equal
func (ia *ipAttributes) Equal(other *ipAttributes) bool {

	return ia.ID != "" &&
		ia.ID == other.ID &&
		ia.LowerBound == other.LowerBound &&
		ia.UpperBound == other.UpperBound &&
		ia.IP.Equal(other.IP) &&
		ia.Reason == other.Reason
}

// IPInt64 returns the IP's int64 value
func (ia *ipAttributes) IPInt64() int64 {
	val, err := ipToInt64(ia.IP)
	if err != nil {
		panic(err)
	}
	return val
}

// EqualIP returns true if the IPs of both are equal
func (ia *ipAttributes) EqualIP(other *ipAttributes) bool {
	return ia.IP.Equal(other.IP)
}

// IsInfBoundary returns true if ia is either the globalUpperBoundary or the globalLowerBoundary
func (ia *ipAttributes) IsInfBoundary() bool {
	return ia.Equal(globalLowerBoundary) || ia.Equal(globalUpperBoundary)
}

// IsSingleBoundary returns true
func (ia *ipAttributes) IsSingleBoundary() bool {
	if ia.LowerBound != ia.UpperBound {
		return true
	} else if ia.LowerBound && ia.UpperBound {
		return false
	}
	panic(errors.New("did not expect both boundaries to be false"))
}

func (ia *ipAttributes) String() string {
	b, err := json.Marshal(ia)
	if err != nil {
		panic(fmt.Errorf("failed to marshal ipAttributes: %w", err))
	}
	return string(b)
}
