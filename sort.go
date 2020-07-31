package goripr

type byIP []boundary

func (a byIP) Len() int      { return len(a) }
func (a byIP) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byIP) Less(i, j int) bool {
	aID := a[i].ID
	bID := a[j].ID

	if aID == "-inf" || bID == "+inf" {
		return true
	} else if aID == "+inf" || bID == "-inf" {
		return false
	}

	aInt := a[i].Int64
	bInt := a[j].Int64

	return aInt < bInt
}
