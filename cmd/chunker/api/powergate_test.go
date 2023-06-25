package api

import (
	"fmt"
	"io"
	"os"
	"testing"
	"time"
)

const POWHOST = "10.80.7.14:5001"

func TestSendToPowerGate(t *testing.T) {
	f, err := os.Open("/home/lichenglin/EasyConnect_x64.deb")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	tm := time.Now()
	cid, err := SendToPowerGate("61.164.212.212:30015", "923eefe7-a8a7-435a-bad0-d4ecc9d415bd", f)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("cid:", cid, " use time:", time.Since(tm))
}

func TestGetColdFile(t *testing.T) {
	f, err := os.OpenFile("EasyConnect_x64.mp4", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
		return
	}
	cid := "QmVqvPkphyWteEquPWZLa4v1TisazAeTvgD8X1KkMAJUJH"
	r, err := GetDataFromCold("61.164.212.212:30015", "923eefe7-a8a7-435a-bad0-d4ecc9d415bd", cid)
	if err != nil {
		fmt.Println(err)
		return
	}
	_, err = io.Copy(f, r)
	if err != nil {
		fmt.Println(err)
		return
	}
}
