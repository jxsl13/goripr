package main

import (
	"encoding/json"
	"fmt"
	"net"
)

var (
	// GlobalLowerBoundary is the smallest value possible that indicates the lower boundary of the IP set
	GlobalLowerBoundary = &IPAttributes{ID: "-inf", LowerBound: false, UpperBound: false}

	//GlobalUpperBoundary is the biggest value possible that indicates the upper boundary of the IP set
	GlobalUpperBoundary = &IPAttributes{ID: "+inf", LowerBound: false, UpperBound: false}
)

// IPAttributes is the composite result type of anything requesting an IP
type IPAttributes struct {
	ID         string
	IP         net.IP
	Reason     string
	LowerBound bool
	UpperBound bool
}

// Equal tests if two attribute instances are equal
func (ia *IPAttributes) Equal(other *IPAttributes) bool {

	return ia.ID != "" &&
		ia.ID == other.ID &&
		ia.LowerBound == other.LowerBound &&
		ia.UpperBound == other.UpperBound &&
		ia.IP.Equal(other.IP) &&
		ia.Reason == other.Reason
}
func (ia *IPAttributes) String() string {
	b, err := json.Marshal(ia)
	if err != nil {
		panic(fmt.Errorf("failed to marshal IPAttributes: %w", err))
	}
	return string(b)
}

// IPRangeAttributes maps an IP range to the Reason string
type IPRangeAttributes struct {
	Range  string
	Reason string // Ban reason
}
