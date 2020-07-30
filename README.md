# Go Redis IP Ranges (goripr)

[![Test](https://github.com/jxsl13/goripr/workflows/Test/badge.svg)](https://github.com/jxsl13/goripr/actions) [![Go Report Card](https://goreportcard.com/badge/github.com/jxsl13/goripr)](https://goreportcard.com/report/github.com/jxsl13/goripr) [![GoDoc](https://godoc.org/github.com/jxsl13/goripr?status.svg)](https://godoc.org/github.com/jxsl13/goripr) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT) [![codecov](https://codecov.io/gh/jxsl13/goripr/branch/master/graph/badge.svg)](https://codecov.io/gh/jxsl13/goripr) [![Total alerts](https://img.shields.io/lgtm/alerts/g/jxsl13/goripr.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/jxsl13/goripr/alerts/)
[![codebeat badge](https://codebeat.co/badges/4b5339f2-93d6-4242-96a6-0372e66a7aaf)](https://codebeat.co/projects/github-com-jxsl13-goripr-master) [![deepsource](https://static.deepsource.io/deepsource-badge-light.svg)](https://deepsource.io/gh/jxsl13/goripr/)

**goripr** is an eficient way to store ip ranges in a redis database and mapping those ranges to specific strings.

This package wraps the widely used redis Go client and extends its feature set with a storage efficient mapping of IPv4 IP ranges to specific strings called reason.

I intend to use this package in my VPN Detection, that's why the term "reason" is used.
The string can be used in any other way needed, especially cotaining JSON formatted data.

## TODO

 -Cache requested IPs for like 24 hours in order to improve response time performance.