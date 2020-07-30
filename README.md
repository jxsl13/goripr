# Go Redis IP Ranges (goripr)

[![Go Report Card](https://goreportcard.com/badge/github.com/jxsl13/goripr)](https://goreportcard.com/report/github.com/jxsl13/goripr)

**goripr** is an eficient way to store ip ranges in a redis database and mapping those ranges to specific strings.

This package wraps the widely used redis Go client and extends its feature set with a storage efficient mapping of IPv4 IP ranges to specific strings called reason.

I intend to use this package in my VPN Detection, that's why the term "reason" is used.
The string can be used in any other way needed, especially cotaining JSON formatted data.

## TODO

 -Cache requested IPs for like 24 hours in order to improve response time performance.