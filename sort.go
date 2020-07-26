package main

type byAttributeIP []*IPAttributes

func (a byAttributeIP) Len() int      { return len(a) }
func (a byAttributeIP) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byAttributeIP) Less(i, j int) bool {
	aInt, _ := IPToInt(a[i].IP)
	bInt, _ := IPToInt(a[j].IP)

	return aInt.Cmp(bInt) < 0
}
