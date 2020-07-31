package goripr

var (

	// IPRangesKey contains the key name of the sorted set that contains the IPs (integers)
	IPRangesKey = "________________IP_RANGES________________"

	// DeleteReason is given to a specific deltion range
	// on a second attept (not aromic) the range is then finally deleted.
	DeleteReason = "_________________DELETE_________________"

	// globalLowerBoundary is the smallest value possible that indicates the lower boundary of the IP set
	globalLowerBoundary = &ipAttributes{ID: "-inf", LowerBound: false, UpperBound: true}

	//globalUpperBoundary is the biggest value possible that indicates the upper boundary of the IP set
	globalUpperBoundary = &ipAttributes{ID: "+inf", LowerBound: true, UpperBound: false}
)

const (

	// ErrConnectionFailed is returned when the connection to the redis database fails.
	ErrConnectionFailed = Error("failed to establish a connection to the redis database")

	// ErrDatabaseInit is returned when the initialization of the database boundaries fails.
	ErrDatabaseInit = Error("failed to initialize database ±inf boundaries")

	// ErrInvalidRange is returned when a passed string is not a valid range
	ErrInvalidRange = Error("invalid range passed, use eithe rof these: <IP>, <IP>/<0-32>, <IP> - <IP>")

	// ErrIPv6NotSupported is returned if an IPv6 range or IP input is detected.
	ErrIPv6NotSupported = Error("IPv6 ranges are not supported")

	// ErrInvalidIP is returned when the passed argument is an invalid IP
	ErrInvalidIP = Error("invalid IP passed")

	// ErrNoResult is returned when a result slic is empty or some connection error occurs during retrieval of values.
	ErrNoResult = Error("could not retrieve any results from the database")

	// ErrIPNotFoundAbove is returned if no IP was found above the requested one
	ErrIPNotFoundAbove = Error("did not find any ips above the requested ip")

	// ErrIPNotFoundBelow is returned if no IP was found below the requested one
	ErrIPNotFoundBelow = Error("did not find any IPs below the requested ip")

	// ErrIPNotFoundInAnyRange is returned if the passed IP is not contained in any ranges
	ErrIPNotFoundInAnyRange = Error("the given IP was not found in any database ranges")

	// ErrUpperBoundary is returned if the next higher IP is +inf
	ErrUpperBoundary = Error("next higher IP is the +inf boundary")

	// ErrLowerBoundary is returned if the next lower IP is -inf
	ErrLowerBoundary = Error("next lower IP is the -inf boundary")
)
