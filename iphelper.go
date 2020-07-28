package main

import (
	"fmt"
	"math/big"
	"net"
	"regexp"
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

// Boundaries returns the lower and upper bound of a given range string
func Boundaries(ipRange string) (low, high net.IP, err error) {

	if matches := ipCidrRegex.FindStringSubmatch(ipRange); len(matches) == 2 {

		var netRange *net.IPNet

		_, netRange, err = net.ParseCIDR(matches[1])
		if err != nil {
			return nil, nil, fmt.Errorf("%w : %v", ErrInvalidRange, err)
		}
		low, high = AddressRange(netRange)

	} else if matches := ipRangeRegex.FindStringSubmatch(ipRange); len(matches) == 3 {

		low = net.ParseIP(matches[1])
		high = net.ParseIP(matches[2])

		if low == nil || high == nil {
			return nil, nil, fmt.Errorf("%w : %s", ErrInvalidRange, "one of the two IPs is invalid")
		}

		lowInt, _ := IPToInt(low)
		highInt, _ := IPToInt(high)

		cmp := lowInt.Cmp(highInt)

		if cmp > 0 {
			return nil, nil, fmt.Errorf("%w : %s", ErrInvalidRange, "first IP must be smaller than second or equal to")
		}

		// low & high are valid!
	} else if matches := ipRegex.FindStringSubmatch(ipRange); len(matches) == 2 {

		ip := net.ParseIP(matches[1])

		if ip == nil {
			return nil, nil, fmt.Errorf("%w : %s", ErrInvalidRange, "invalid IP")
		}

		// low & high are valid
		low, high = ip, ip
	} else {
		return nil, nil, ErrInvalidRange
	}

	// force IPv4
	low = low.To4()
	high = high.To4()
	if low == nil || high == nil {
		return nil, nil, ErrIPv6NotSupported
	}

	// low, high, nil
	return
}

// AddressRange returns the first and last addresses in the given CIDR range.
func AddressRange(network *net.IPNet) (net.IP, net.IP) {
	// the first IP is easy
	firstIP := network.IP

	// the last IP is the network address OR NOT the mask address
	prefixLen, bits := network.Mask.Size()
	if prefixLen == bits {
		// Easy!
		// But make sure that our two slices are distinct, since they
		// would be in all other cases.
		lastIP := make([]byte, len(firstIP))
		copy(lastIP, firstIP)
		return firstIP, lastIP
	}

	firstIPInt, bits := IPToInt(firstIP)
	hostLen := uint(bits) - uint(prefixLen)
	lastIPInt := big.NewInt(1)
	lastIPInt.Lsh(lastIPInt, hostLen)
	lastIPInt.Sub(lastIPInt, big.NewInt(1))
	lastIPInt.Or(lastIPInt, firstIPInt)

	return firstIP, IntToIP(lastIPInt, bits)
}

// IPToInt returns the IP as bigInt as well as the number of occupied
func IPToInt(ip net.IP) (*big.Int, int) {
	val := &big.Int{}
	val.SetBytes([]byte(ip))

	leadingZeroBytes := 0
	for _, b := range ip {
		if b == 0 {
			leadingZeroBytes++
		} else {
			break
		}
	}

	occupiedBytes := len(ip) - leadingZeroBytes
	if occupiedBytes == net.IPv4len && len(ip) == net.IPv4len {
		return val, IPv4Bits
	}
	// if statements in ascending length order
	if occupiedBytes == net.IPv4len+2 && len(ip) == net.IPv6len && ip[10] == 255 && ip[11] == 255 {
		return val, IPv4Bits
	} else if occupiedBytes <= net.IPv6len {
		return val, IPv6Bits
	} else {
		panic(fmt.Errorf("Unsupported address length %d", len(ip)))
	}
}

// IntToIP converts a bigInt to an IP address
func IntToIP(ipInt *big.Int, ipBits int) net.IP {
	size := ipBits / 8
	ipBytes := ipInt.Bytes()

	if len(ipBytes) < size {
		size = len(ipBytes)
	}

	if size == 6 {
		size = IPv4Bits / 8
	}

	ret := make([]byte, size)
	// Pack our IP bytes into the end of the return array,
	// since big.Int.Bytes() removes front zero padding.
	for i := 1; i <= size; i++ {
		ret[size-i] = ipBytes[len(ipBytes)-i]
	}
	return net.IP(ret)
}
