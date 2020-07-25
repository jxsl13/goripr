package main

import "errors"

const (
	// Boundary represents the infinity boundaries that are not IPs
	Boundary = 1 << iota
	// BeginRange identifies an IP value as begginning of a range.
	BeginRange

	// EndRange identifies an IP to represent the end of a range.
	EndRange

	// BeginAndEndRange identifies an IP to be the IP range itself.
	BeginAndEndRange
)

var (

	// IPRangesKey contains the key name of the sorted set that contains the IPs (integers)
	IPRangesKey = "________________IP_RANGES________________"

	// BoundaryAttributeMapKey is the name of the object that maps uuids of range boundaries to specific reasons
	// also maps a boundaries' type, like beginning, end of range, etc.
	BoundaryAttributeMapKey = "________________REASON_MAP_______________"

	// ErrConnectionFailed is returned when the connection to the redis database fails.
	ErrConnectionFailed = errors.New("failed to establish a connection to the redis database")

	// ErrDatabaseInit is returned when the initialization of the database boundaries fails.
	ErrDatabaseInit = errors.New("failed to initialize database ±inf boundaries")

	// ErrInvalidRange is returned when a passed string is not a valid range
	ErrInvalidRange = errors.New("invalid range passed, use eithe rof these: <IP>, <IP>/<0-32>, <IP> - <IP>")

	// ErrIPv6NotSupported is returned if an IPv6 range or IP input is detected.
	ErrIPv6NotSupported = errors.New("IPv6 ranges are not supported")

	// ErrInvalidIP is returned when the passed argument is an invalid IP
	ErrInvalidIP = errors.New("invalid IP passed")

	// ErrNoResult is returned when a result slic is empty or some connection error occurs during retrieval of values.
	ErrNoResult = errors.New("could not retrieve any results from the database")

	// ErrIPNotFoundAbove is returned if no IP was found above the requested one
	ErrIPNotFoundAbove = errors.New("did not find any ips above the requested ip")

	// ErrIPNotFoundBelow is returned if no IP was found below the requested one
	ErrIPNotFoundBelow = errors.New("did not find any IPs below the requested ip")

	// ErrUpperBoundary is returned if the next higher IP is +inf
	ErrUpperBoundary = errors.New("next higher IP is the +inf boundary")

	// ErrLowerBoundary is returned if the next lower IP is -inf
	ErrLowerBoundary = errors.New("next lower IP is the -inf boundary")
)
