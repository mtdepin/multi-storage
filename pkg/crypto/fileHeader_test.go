package crypto

import (
	"testing"
)

func TestName(t *testing.T) {
	a := []byte("1233")
	info := AddHeader(a)
	t.Log(string(info[:]))
	//b := []byte("e6edc421c0c7c6bf326ccd5e010fb115c217fe4a")
	t.Log(len(a))
	t.Log(string(info[40:43]))
	t.Log(CheckHeader(info))
}
func TestOrSize(t *testing.T) {
	a := int64(3956640)
	size, err := OrSize(a)
	t.Log(size)
	if err != nil {
		return
	}
}
