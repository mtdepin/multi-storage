package engine

import (
	"context"
	shell "github.com/ipfs/go-ipfs-api"
	"io"
)

type StorageConfig struct {
	Provider    string
	Replication int
	Targets     map[string]EngineConfig
}

type EngineConfig struct {
	Endpoints map[string]*EndpointConfig
}

type EndpointConfig struct {
	Http     int
	Fix      int
	CanStore bool
	CanRead  bool
	health   bool
}

type RepoStat struct {
	NumObjects uint64
	// RepoPath   uint64
	RepoSize   uint64
	StorageMax uint64
	Version    string
}

type Storage interface {
	Start() error
	//Write save file
	Write(ctx context.Context, file io.Reader) (string, error)
	//Read : read file
	Read(ctx context.Context, cid string) (io.Reader, error)
	//Delete delete file
	Delete(ctx context.Context, cid string) error
	//Stat get file stat
	Stat(ctx context.Context, cid string) error
	//DagTree get file dag tree
	DagTree(ctx context.Context, cid string) (interface{}, error)
	//DagSize get dag size
	DagSize(ctx context.Context, cid string) (string, error)
	//Pin pin a file
	Pin(path string) error
	//Unpin unpin a file
	Unpin(path string) error

	//List directory contents for Unix filesystem objects.
	List(cid string) ([]*shell.LsLink, error)
	RepoStat() (RepoStat, error)
}
