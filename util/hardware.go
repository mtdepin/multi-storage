package util

import (
	"context"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/net"

	"sync"
	"time"
)

const timesPerSecond = 1

type NetSpeed struct {
	lastSent    uint64
	lastReceive uint64
	kBSpeed     float64
	lock        sync.Mutex
	ctx         context.Context
}

func NewNetSpeed(ctx context.Context) *NetSpeed {
	ns := NetSpeed{
		lastSent:    0,
		lastReceive: 0,
		kBSpeed:     0,
		lock:        sync.Mutex{},
		ctx:         ctx,
	}
	ns.Collect()
	return &ns
}

func (n *NetSpeed) Collect() {
	go func() {
		ticker := time.NewTicker(time.Second / timesPerSecond)
		for {
			select {
			case <-ticker.C:
				sentBytes, receivedBytes := n.update()
				n.lock.Lock()
				n.kBSpeed = float64(sentBytes+receivedBytes) * float64(timesPerSecond) / 1024
				n.lock.Unlock()
			case <-n.ctx.Done():
				ticker.Stop()
				return
			}
		}
	}()
}

func (n *NetSpeed) KBSpeed() float64 {
	n.lock.Lock()
	speed := n.kBSpeed
	n.lock.Unlock()
	return speed
}

func (n *NetSpeed) update() (sentBytes, receivedBytes uint64) {
	states, err := net.IOCounters(false)
	if err != nil || len(states) == 0 {
		return
	}
	state := states[0]
	defer func() {
		n.lastSent = state.BytesSent
		n.lastReceive = state.BytesRecv
	}()
	if n.lastSent == 0 || n.lastReceive == 0 {
		return
	}
	sent := state.BytesSent - n.lastSent
	receive := state.BytesRecv - n.lastReceive
	return sent, receive
}

func NewDiskCollect() chan *disk.UsageStat {
	c := make(chan *disk.UsageStat)
	go func() {
		ticker := time.NewTicker(time.Second / timesPerSecond)
		for range ticker.C {
			ust, err := disk.Usage("./")
			if err == nil {
				c <- ust
			}
		}
	}()
	return c
}
