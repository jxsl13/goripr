package goripr

import (
	"github.com/go-redis/redis"
)

type byAttributeIP []*IPAttributes

func (a byAttributeIP) Len() int      { return len(a) }
func (a byAttributeIP) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byAttributeIP) Less(i, j int) bool {
	aID := a[i].ID
	bID := a[j].ID

	if aID == "-inf" || bID == "+inf" {
		return true
	} else if aID == "+inf" || bID == "-inf" {
		return false
	}

	aInt, _ := IPToInt64(a[i].IP)
	bInt, _ := IPToInt64(a[j].IP)

	return aInt < bInt
}

type byScore []redis.Z

func (a byScore) Len() int      { return len(a) }
func (a byScore) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byScore) Less(i, j int) bool {
	return a[i].Score < a[j].Score
}
