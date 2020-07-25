package main

import (
	"encoding/json"
	"fmt"
	"net"
)

// IPAttributes is the composite result type of anything requesting an IP
type IPAttributes struct {
	ID         string
	IP         net.IP
	Reason     string
	LowerBound bool
	UpperBound bool
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

// // Z returns a rdis.Z object to be used with ZAdd etc.
// func (z *ZMember) Z() redis.Z {
// 	integer, _ := IPToInt(z.IP)

// 	return redis.Z{
// 		Score:  float64(integer.Int64()),
// 		Member: z.ID,
// 	}
// }

// // FromZ fills the object with the apprpriate data from the response of the redis database
// func (z *ZMember) FromZ(rz redis.Z) error {
// 	switch s := rz.Member.(type) {
// 	case string:
// 		z.ID = s
// 	default:
// 		return fmt.Errorf("redis.Z response 'Member' is not of type string")
// 	}

// 	if rz.Score == math.Inf(-1) {
// 		return ErrLowerBoundary
// 	} else if rz.Score == math.Inf(1) {
// 		return ErrUpperBoundary
// 	}

// 	intIP := big.NewInt(int64(rz.Score))
// 	z.IP = IntToIP(intIP, IPv4Bits)
// 	return nil
// }

// // IPLen returns the length of the ip in BITS
// func (z *ZMember) IPLen() int {
// 	return 8 * len(z.IP)
// }
