package util

import (
	"fmt"
	"github.com/shirou/gopsutil/net"
	"testing"
)

func TestNewDiskCollect(t *testing.T) {
	states, err := net.IOCounters(false)
	fmt.Println(states, err)
}

func TestHet(t *testing.T) {

}
