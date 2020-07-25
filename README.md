# Efficient  IP range to string mapping using the Redis database

This package wraps the widely used redis Go client and extends its feature set with a storage efficient mapping of IPv4 IP ranges to specific strings called reason.

I intend to use this package in my VPN Detection, that's why the term "reason" is used.
The string can be used in any other way needed, especially cotaining JSON formatted data.
