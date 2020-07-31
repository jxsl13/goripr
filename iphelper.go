package goripr

import (
	"fmt"
	"net"
	"regexp"

	"github.com/xgfone/netaddr"
)

var (
	ipCidrRegex  = regexp.MustCompile(`([0-9a-f:.]{7,41}/\d{1,3})`)
	ipRangeRegex = regexp.MustCompile(`([0-9a-f:.]{7,41})\s*-\s*([0-9a-f:.]{7,41})`)
	ipRegex      = regexp.MustCompile(`([0-9a-f:.]{7,41})`)
)

const (
	// IPv4Bits is the number of occupied bits by IPv4
	IPv4Bits = 32
	// IPv6Bits is the number of occupied bits by IPv6
	IPv6Bits = 128
)

// boundaries returns the lower and upper bound of a given range string
func boundaries(ipRange string) (low, high net.IP, err error) {

	if matches := ipCidrRegex.FindStringSubmatch(ipRange); len(matches) == 2 {

		net, err := netaddr.NewIPNetwork(matches[1])
		if err != nil {
			return nil, nil, fmt.Errorf("%w : %v", ErrInvalidRange, err)
		}

		low, high = net.First().IP(), net.Last().IP()

	} else if matches := ipRangeRegex.FindStringSubmatch(ipRange); len(matches) == 3 {

		lowAddr, err := netaddr.NewIPAddress(matches[1])
		if err != nil {
			return nil, nil, fmt.Errorf("%w : %v", ErrInvalidRange, err)
		}
		highAddr, err := netaddr.NewIPAddress(matches[2])

		if err != nil {
			return nil, nil, fmt.Errorf("%w : %v", ErrInvalidRange, err)
		}

		if lowAddr.Compare(highAddr) > 0 {
			return nil, nil, fmt.Errorf("%w : %s", ErrInvalidRange, "first IP must be smaller than second or equal to")
		}

		// low & high are valid!
		low, high = lowAddr.IP(), highAddr.IP()

	} else if matches := ipRegex.FindStringSubmatch(ipRange); len(matches) == 2 {

		ip, err := netaddr.NewIPAddress(matches[1])

		if err != nil {
			return nil, nil, fmt.Errorf("%w : %s", ErrInvalidRange, "invalid IP")
		}

		// low & high are valid
		low, high = ip.IP(), ip.IP()
	} else {
		return nil, nil, ErrInvalidRange
	}

	// force IPv4
	low = low.To4()
	high = high.To4()
	if low == nil || high == nil {
		return nil, nil, ErrIPv6NotSupported
	}

	return low, high, nil
}

// ipToInt64 returns th einteger representation of the passed IP
func ipToInt64(ip net.IP) (int64, error) {
	ipAddr, err := netaddr.NewIPAddress(ip)
	if err != nil {
		return 0, err
	}

	return ipAddr.BigInt().Int64(), nil
}

// int64ToIP returns an ip from the passed integer
func int64ToIP(i int64) (net.IP, error) {
	ipAddr, err := netaddr.NewIPAddress(i)
	if err != nil {
		return nil, err
	}

	return ipAddr.IP(), nil
}

// float64ToIP returns an ip from the passed integer
func float64ToIP(i float64) (net.IP, error) {
	ipAddr, err := netaddr.NewIPAddress(int64(i))
	if err != nil {
		return nil, err
	}

	return ipAddr.IP(), nil
}

// ipToFloat64 returns th einteger representation of the passed IP
func ipToFloat64(ip net.IP) (float64, error) {
	ipAddr, err := netaddr.NewIPAddress(ip)
	if err != nil {
		return 0, err
	}
	return float64(ipAddr.BigInt().Int64()), nil
}
