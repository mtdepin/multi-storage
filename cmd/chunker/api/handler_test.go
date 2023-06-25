package api

import (
	"fmt"
	"math"
	"net/http"
	"path"
	"strconv"
	"strings"
	"testing"

	shell "github.com/ipfs/go-ipfs-api"
)

var chunker = chunkerAPIHandlers{}

func TestMockServer(t *testing.T) {
	http.HandleFunc("/cs/v1/object", chunker.PutObjectHandler)
	http.HandleFunc("/cs/v1/object/", chunker.GetObjectHandler)
	t.Fatal(http.ListenAndServe("127.0.0.1:8081", nil))
}

func TestA(t *testing.T) {
	fmt.Println(path.Base("/hello.txt"))
}

func TestDag(t *testing.T) {
	ipfsNode := shell.NewShell("http://192.168.1.194:5001")
	var res interface{}
	ipfsNode.DagGet("QmRFZG5kTyzt6BPsv3QhuUvuLFGqRTP67hgDJoJfpYHTxX", &res)
	b := float64(0)
	a := res.(map[string]interface{})["Links"].([]interface{})
	for i := range a {
		b += a[i].(map[string]interface{})["Tsize"].(float64)
	}
	res.(map[string]interface{})["Data"].(map[string]interface{})["size"] = b
	fmt.Println(b)

}

func TestName(t *testing.T) {
	arr := []string{"0-9", "10-19", "20-29", "30-39", "40-49", "50-59", "60-69", "70-79", "80-89"}
	rang := "10-60"
	start, end := para(rang)
	si := findIndex(start, 10)
	ei := findIndex(end, 10)
	s, _ := para(arr[si])
	_, e := para(arr[ei])
	t.Log(si, ei, s, e)

}

func para(rang string) (start, end int64) {
	before, after, found := strings.Cut(rang, "-")
	if found {
		start, _ = strconv.ParseInt(before, 10, 64)
		end, _ = strconv.ParseInt(after, 10, 64)
	}
	return
}

func findIndex(number int64, section int) int64 {
	i := number / int64(section)
	return int64(math.Ceil(float64(i)))
}
