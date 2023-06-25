package metadata

import (
	"sync"
	"time"

	"github.com/jinzhu/gorm"
)

type BucketInfo struct {
	gorm.Model   `json:"-"`
	Bucketid     string `gorm:"column:bucketid;type:varchar(32);not null" json:"bucketid"`
	Name         string `gorm:"column:name;type:varchar(64);not null;primary_key" json:"name"`
	Count        uint64 `gorm:"column:count;type:bigint;default:0" json:"count"`
	Size         uint64 `gorm:"column:size;type:bigint;default:0" json:"size"`
	Owner        uint32 `gorm:"column:owner;type:int;default:null" json:"owner"`
	Tenant       string `gorm:"column:tenant;type:varchar(32);default:null" json:"tenant"`
	Policy       string `gorm:"column:policy;type:varchar(1024);default:null" json:"-"`
	Versioning   string `gorm:"column:versioning;type:varchar(16);default:null" json:"versioning"`
	Profile      string `gorm:"column:profile;type:varchar(1024)" json:"profile"`
	Encryption   string `gorm:"column:encryption;type:varchar(64)" json:"encryption"`
	StorageClass string `gorm:"column:storageclass;type:varchar(32)" json:"storageclass,omitempty"`
	Location     string `gorm:"column:location;type:varchar(64)" json:"location,omitempty"`
}

type BucketExternal struct {
	gorm.Model `json:"-"`
	Name       string `gorm:"column:name;type:varchar(64);not null;primary_key" json:"name"`
	Tag        string `gorm:"column:tag;type:varchar(4096)" json:"tag"`
	Log        string `gorm:"column:log;type:varchar(2048)" json:"log"`
	Acl        string `gorm:"column:acl;type:varchar(1024)" json:"acl"`
	Policy     string `gorm:"column:policy;type varchar(20480)" json:"policy"`
	Lifecycle  string `gorm:"column:lifecycle;type varchar(1024)" json:"lifecycle"`
}

type ObjectInfo struct {
	gorm.Model
	Name           string `gorm:"column:name;type:varchar(512);not null;index:o_n_index;primary_key" json:"name"`
	Dirname        string `gorm:"column:dirname;type:varchar(1024);not null;default:'/'" json:"dirname"`
	Bucket         string `gorm:"column:bucket;type:varchar(64);not null" json:"bucket"`
	Cid            string `gorm:"column:cid;type:varchar(160)" json:"cid,omitempty"`
	Etag           string `gorm:"column:etag;type:varchar(32)" json:"etag,omitempty"`
	Content_length uint64 `gorm:"column:content_length;type:bigint" json:"content_length"`
	CipherTextSize uint64 `gorm:"column:ciphertext_size;type:bigint" json:"ciphertext_size"`
	Content_type   string `gorm:"column:content_type;type:varchar(128)" json:"content_type"`
	Version        string `gorm:"column:version;type:varchar(32)" json:"version"`
	Tags           string `gorm:"column:tags;type:varchar(1024)" json:"tags,omitempty"`
	Isdir          bool   `gorm:"column:isdir;type:bool;default:false" json:"isdir"`
	IsMarker       bool   `gorm:"column:ismarker;type:bool;default:false" json:"ismarker"`
	StorageClass   string `gorm:"column:storageclass;type:varchar(32)" json:"storageclass"`
	Acl            string `gorm:"column:acl;type:varchar(1024)" json:"acl"`
}

type ObjectHistoryInfo struct {
	gorm.Model
	Name           string `gorm:"column:name;type:varchar(512);not null"`
	Dirname        string `gorm:"column:dirname;type:varchar(1024);not null;default:'/'"`
	Bucket         string `gorm:"column:bucket;type:varchar(64);not null"`
	Cid            string `gorm:"column:cid;type:varchar(160)"`
	Etag           string `gorm:"column:etag;type:varchar(32)"`
	Content_length uint64 `gorm:"column:content_length;type:bigint"`
	CipherTextSize uint64 `gorm:"column:ciphertext_size;type:bigint" json:"ciphertext_size"`
	Content_type   string `gorm:"column:content_type;type:varchar(128)"`
	Version        string `gorm:"column:version;type:varchar(32)"`
	Tags           string `gorm:"column:tags;type:varchar(1024)"`
	Isdir          bool   `gorm:"column:isdir;type:bool;default:false"`
	IsMarker       bool   `gorm:"column:ismarker;type:bool;default:false" json:"ismarker"`
	StorageClass   string `gorm:"column:storageclass;type:varchar(32)" json:"storageclass,omitempty"`
	Acl            string `gorm:"column:acl;type:varchar(1024)" json:"acl"`
}

type ObjectChunkInfo struct {
	gorm.Model
	Bucket   string `gorm:"column:bucket;type:varchar(64);not null"`
	Name     string `gorm:"column:name;type:varchar(512);not null"`
	Cid      string `gorm:"column:cid;type:varchar(160);not null"`
	IpIDTree string `gorm:"column:ipld_tree;type:text;not null"`
	IpIDMap  string `gorm:"column:ipld_map;type:text;not null"`
	// Status   int    `gorm:"column:status;type:int;default:0"`
}

// bucket info for api
type StorageInfo struct {
	BucketsNum int    `json:"bucketnum"`
	ObjectNum  uint64 `json:"objectnum"`
	TotalSzie  uint64 `json:"totalsize"`
}

type ObjectOptions struct {
	Bucket, Prefix, Object, VersionID string
	CurrentVersion, HistoryVersion    bool
	IsDir                             bool
}

type DeletedObjects struct {
	Count, Size uint64
}

const (
	BucketTable        = "t_ns_bucket"
	BucketExtTable     = "t_ns_bucket_ext"
	ObjectTable        = "t_ns_object"
	ObjectHistoryTable = "t_ns_object_history"
	ObjectCidTable     = "t_ns_object_chunk"
)

// bucket versionning status
const (
	VersioningEnabled   = "Enabled"
	VersioningSuspended = "Suspended"
	VersioningUnset     = "Null"
)

// object default value definition
const (
	Defaultversionid = "null"
	DefaultCid       = "-"
	DefaultEtag      = "-"
	DirContentType   = "dir"
	DefaultOjbectACL = "bucket"
)

const (
	MaxObjectVersion = 1000
	MaxBucketNum     = 1000
)

// locker for delete and put
var bucketWriteLock = sync.Mutex{}

// some common functions
func now() time.Time {
	return time.Now().UTC()
}
