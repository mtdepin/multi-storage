package util

import "time"

const (
	State_Health  = "ok"
	State_keep    = "keep"
	State_Offline = "offline"
)

type Region struct {
	RegionId int64  `json:"id"`
	Name     string `json:"name"`
}

type ReWriteObjectInfo struct {
	Name           string
	DirName        string
	Bucket         string
	Cid            string
	Etag           string
	ContenLength   uint64
	CipherTextSize uint64
	Size           int64
	IsDir          bool
	StorageClass   string
	ContentType    string
	ActualCid      string
	ACL            string
}

type ChunkerNodeInfo struct {
	Id             string    `json:"id"`
	UUID           string    `json:"uuid,omitempty"`
	Endpoint       string    `json:"url"`
	Tcp            string    `json:"tcp"`
	Region         *Region   `json:"region"`
	TotalSpace     uint64    `json:"totalspace,omitempty"`
	UsedSpace      uint64    `json:"usedspace,omitempty"`
	AvailableSpace uint64    `json:"availspace,omitempty"`
	State          string    `json:"state,omitempty"`
	LatestNetSpeed float64   `json:"latestNetSpeed"` //最新网速
	Time           time.Time `json:"time"`
}

type Disk struct {
	Endpoint        string  `json:"endpoint,omitempty"`
	RootDisk        bool    `json:"rootDisk,omitempty"`
	DrivePath       string  `json:"path,omitempty"`
	Healing         bool    `json:"healing,omitempty"`
	State           string  `json:"state,omitempty"`
	UUID            string  `json:"uuid,omitempty"`
	Model           string  `json:"model,omitempty"`
	TotalSpace      uint64  `json:"totalspace,omitempty"`
	UsedSpace       uint64  `json:"usedspace,omitempty"`
	AvailableSpace  uint64  `json:"availspace,omitempty"`
	ReadThroughput  float64 `json:"readthroughput,omitempty"`
	WriteThroughPut float64 `json:"writethroughput,omitempty"`
	ReadLatency     float64 `json:"readlatency,omitempty"`
	WriteLatency    float64 `json:"writelatency,omitempty"`
	Utilization     float64 `json:"utilization,omitempty"`
	FreeInodes      uint64  `json:"free_inodes,omitempty"`

	// Indexes, will be -1 until assigned a set.
	PoolIndex int `json:"pool_index"`
	SetIndex  int `json:"set_index"`
	DiskIndex int `json:"disk_index"`
}

type Event struct {
	EventType string    `json:"id"`
	Payload   []byte    `json:"payload"`
	Time      time.Time `json:"time"`
}
