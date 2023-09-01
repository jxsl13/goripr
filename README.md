# Go Redis IP Ranges (goripr)

[![Test](https://github.com/jxsl13/goripr/actions/workflows/test.yaml/badge.svg)](https://github.com/jxsl13/goripr/actions) [![Go Report Card](https://goreportcard.com/badge/github.com/jxsl13/goripr)](https://goreportcard.com/report/github.com/jxsl13/goripr) [![GoDoc](https://godoc.org/github.com/jxsl13/goripr?status.svg)](https://godoc.org/github.com/jxsl13/goripr) [![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT) [![codecov](https://codecov.io/gh/jxsl13/goripr/branch/master/graph/badge.svg)](https://codecov.io/gh/jxsl13/goripr) [![Sourcegraph](https://sourcegraph.com/github.com/jxsl13/goripr/-/badge.svg)](https://sourcegraph.com/github.com/jxsl13/goripr?badge) [![deepsource](https://static.deepsource.io/deepsource-badge-light.svg)](https://deepsource.io/gh/jxsl13/goripr/)


**goripr** is an eficient way to store IP ranges in a redis database and mapping those ranges to specific strings.

This package wraps the widely used redis Go client and extends its feature set with a storage efficient mapping of IPv4 ranges to specific strings called reasons.

I intend to use this package in my VPN Detection, that's why the term "reason" is used.
The term refers to a ban reason that is given when a player using a VPN (they usually do that with malicious intent) gets banned.
The string can be used in any other way needed, especially containing JSON formatted data.

## Idea

The general approach is to save the beginning and the end of a range into the database.
The beginning boundary has the property called `LowerBound` set to true and the last IP in a given range is called an upper boundary with the property `UpperBound` set to true.
Based on these properties it is possible to determine, how to cut existing boundaries, when new IP ranges are inserted into the database.

## Problem it solves

The VPN detection and especially the ban server used to save all IPs from the given ranges with their corresponding reasons into the database. That is the trivial approach, but proved to be inefficient when having more than 100 million individual IPs stored in the Redis database. At it's peak the database needed ~7GB of RAM, which is not a feasible solution, especially when the source files containing the actual ranges in their respective masked shorthand notation (`x.x.x.x/24`) needed less than one MB of storage space.

## Gains over the trivial approach

On the other hand, iterating over ~50k such range strings was also not a feasible solution, especially when the ban server should react within ~1 second.
The compromise should be a slower reaction time compared to the triavial approach, but way less of a RAM overhead.
I guess that the reduction of RAM usage by a factor of about 240x should also improve the response time significantly, as the ~7GB approach was burdening even high performance servers rather heavily.
The current RAM that is being used is about 30MB, which is acceptable.

## Input format of the package

```txt
# custom IP range
84.141.32.1 - 84.141.32.255

# single IP
84.141.32.1

# subnet mask
84.141.32.1/24
```

## Example

```Go
package main

import (
    "bufio"
    "errors"
    "flag"
    "log"
    "os"
    "regexp"

    "github.com/jxsl13/goripr/v2"
)

var (
    
    splitRegex    = regexp.MustCompile(`([0-9.\-\s/]+)#?\s*(.*)\s*$`)
    defaultReason = "VPN - https://website.com"

    addFile = ""
    findIP  = ""
)

func init() {
    flag.StringVar(&addFile, "add", "", "-add filename.txt")
    flag.StringVar(&findIP, "find", "", "-find 123.0.0.1")
    flag.Parse()

    if addFile == "" && addFile == "" {
        flag.PrintDefaults()
        os.Exit(1)
    }
}

func parseLine(line string) (ip, reason string, err error) {
    if matches := splitRegex.FindStringSubmatch(line); len(matches) > 0 {
        return matches[1], matches[2], nil
    }
    return "", "", errors.New("empty")
}

func addIPsToDatabase(ctx context.Context, filename string) error {
    file, err := os.Open(filename)
    if err != nil {
        return err
    }

    scanner := bufio.NewScanner(file)
    for scanner.Scan() {
        ip, reason, err := parseLine(scanner.Text())
        if err != nil {
            continue
        }
        if reason == "" {
            reason = defaultReason
        }

        err = rdb.Insert(ctx, ip, reason)
        if err != nil {
            if !errors.Is(err, goripr.ErrInvalidRange) {
                log.Println(err, "Input:", ip)
            }
            continue
        }
    }
    return nil
}

func main() {
    ctx := context.Background()
    rdb, err := goripr.NewClient(ctx, goripr.Options{
        Addr: "localhost:6379",
        DB:   0,
    })
    if err != nil {
        log.Println("error:", err)
        os.Exit(1)
    }
    defer rdb.Close()

    if addFile != "" {
        err := addIPsToDatabase(ctx, addFile)
        if err != nil {
            log.Println("error:", err)
            os.Exit(1)
        }
    } else if findIP != "" {
        reason, err := rdb.Find(ctx, findIP)
        if err != nil {
            log.Println("error:", err)
            os.Exit(1)
        }
        log.Println("IP:", findIP, "Reason:", reason)
        return
    }
}
```

### Example text file

```txt
84.141.32.1 - 84.141.32.255 # any range where the first IP is smaller than the second

2.56.92.0/22 # VPN subnet masking

# without a reason (uses default reason)
2.56.140.0/24
```

## TODO

- Optional Cache of requested IPs for like 24 hours in order to improve response time for recurring requests (rejoining players)
