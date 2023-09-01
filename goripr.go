package goripr

var (

	// IPRangesKey contains the key name of the sorted set that contains the IPs (integers)
	IPRangesKey = "________________IP_RANGES________________"

	// DeleteReason is given to a specific deltion range
	// on a second attept (not atomic) the range is then finally deleted.
	DeleteReason = "_________________DELETE_________________"
)

const (

	// ErrConnectionFailed is returned when the connection to the redis database fails.
	ErrConnectionFailed = Error("failed to establish a connection to the redis database")

	// ErrDatabaseInit is returned when the initialization of the database boundaries fails.
	ErrDatabaseInit = Error("failed to initialize database ±inf boundaries")

	// ErrDatabaseInconsistent is returned when the initialization of the database boundaries fails.
	ErrDatabaseInconsistent = Error("the databe is in an inconsistent state")

	// ErrInvalidRange is returned when a passed string is not a valid range
	ErrInvalidRange = Error("invalid range passed, use either of these: <IP>, <IP>/<1-32>, <IP> - <IP>")

	// ErrIPv6NotSupported is returned if an IPv6 range or IP input is detected.
	ErrIPv6NotSupported = Error("IPv6 ranges are not supported")

	// ErrInvalidIP is returned when the passed argument is an invalid IP
	ErrInvalidIP = Error("invalid IP passed")

	// ErrNoResult is returned when a result slic is empty or some connection error occurs during retrieval of values.
	ErrNoResult = Error("could not retrieve any results from the database")

	// ErrIPNotFound is returned if the passed IP is not contained in any ranges
	ErrIPNotFound = Error("the given IP was not found in any database ranges")
)

// Error is a wrapper for constant errors that are not supposed to be changed.
type Error string

func (e Error) Error() string { return string(e) }
