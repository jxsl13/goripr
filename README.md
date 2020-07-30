# Go Redis IP Ranges (goripr)

[![Test](https://github.com/jxsl13/goripr/workflows/Test/badge.svg)](https://github.com/jxsl13/goripr/actions) [![Go Report Card](https://goreportcard.com/badge/github.com/jxsl13/goripr)](https://goreportcard.com/report/github.com/jxsl13/goripr) [![GoDoc](https://godoc.org/github.com/jxsl13/goripr?status.svg)](https://godoc.org/github.com/jxsl13/goripr) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT) [![codecov](https://codecov.io/gh/jxsl13/goripr/branch/master/graph/badge.svg)](https://codecov.io/gh/jxsl13/goripr) [![Total alerts](https://img.shields.io/lgtm/alerts/g/jxsl13/goripr.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/jxsl13/goripr/alerts/)
[![codebeat badge](https://codebeat.co/badges/4b5339f2-93d6-4242-96a6-0372e66a7aaf)](https://codebeat.co/projects/github-com-jxsl13-goripr-master) [![deepsource](https://static.deepsource.io/deepsource-badge-light.svg)](https://deepsource.io/gh/jxsl13/goripr/)

**goripr** is an eficient way to store ip ranges in a redis database and mapping those ranges to specific strings.

This package wraps the widely used redis Go client and extends its feature set with a storage efficient mapping of IPv4 IP ranges to specific strings called reasons.

I intend to use this package in my VPN Detection, that's why the term "reason" is used.
The term refers to ban reason that is given when a player using a VPN (they usually do that with malicious intent).
The string can be used in any other way needed, especially containing JSON formatted data.

The general approach is to save the beginning and the end of a range into the database.
The beginning boundary has the property called `LowerBound` set to true and the last IP in a given range is called an upper boundary with the property `UpperBound` set to true.
Based on these properties it is possible to determine, how to cut existing boundaries, when new Ip ranges are added to the database.

The VPN detection and especially the ban server used to load all IPs from the given ranges with their corresponding reasons into the database. That is the trivial approach, but proved to be inefficient when having more than 100 million individual IPs stored in the Redis database. At it's peak the database needed ~7GB of RAM, which is not a feasible solution, especially when the source files containing the actual ranges in their respective masked shorthand notation (`x.x.x.x/24`) needed less than one MB of storage space.

On the other hand, iterating over ~50k such range strings was also not a feasible solution, especially when ban server should react within ~1 second.
The compromise should be a slower reaction time compared to the triavial approach, but way less of a RAM overhead.

## TODO

 -Cache requested IPs for like 24 hours in order to improve response time performance
 -The current implementation is too complex and has too many cases, shoul dbe refactored in any case.
 -these many cases and rather messy implementation lead to too many unneeded database requests that could be removed to improve performance
 -Add a cleanup function that removes single `UpperBound`s that are followed by `LowerBound`s sharing the exact same reason string (will be more complex, as we support single value ranges)
 -Only expose the functions `Find`, `Insert`, `Remove`, `Optimize/Clean`, encapsulate the redis client and don't give access.
 -As Redis database requests that require reading and writing of data are NOT atomic, the package will have to implement a RWMutex for somewhat making this at least within on application, that uses this package, atomic.
