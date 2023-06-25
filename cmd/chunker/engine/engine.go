package engine

import (
	"context"
	"errors"
	shell "github.com/ipfs/go-ipfs-api"
	"io"
)

type Engine struct {
	config   StorageConfig
	provider Storage
}

const (
	STORAGE_IPFS    = "ipfs"
	STORAGE_CLUSTER = "ipfscluster"
)

func NewEngine(config StorageConfig) (*Engine, error) {
	var engine = &Engine{}
	engine.config = config
	if err := engine.checkConfig(); err != nil {
		return nil, err
	}
	switch engine.config.Provider {
	case STORAGE_IPFS:
		engine.provider = newIpfs(engine.config)
	case STORAGE_CLUSTER:
		engine.provider = newIpfsCluster(engine.config.Targets[STORAGE_CLUSTER])
	}
	return engine, nil
}

func (e *Engine) checkConfig() error {
	if len(e.config.Targets[e.config.Provider].Endpoints) == 0 {
		err := errors.New("storage target endpoint not set")
		return err
	}
	return nil
}

func (e *Engine) Write(ctx context.Context, file io.Reader) (string, error) {
	return e.provider.Write(ctx, file)
}

func (e *Engine) Read(ctx context.Context, cid string) (io.Reader, error) {
	return e.provider.Read(ctx, cid)
}

func (e *Engine) Delete(ctx context.Context, cid string) error {
	return nil
}
func (e *Engine) Stat(ctx context.Context, cid string) error {
	return e.provider.Stat(ctx, cid)
}

func (e *Engine) DagTree(ctx context.Context, cid string) (interface{}, error) {
	return e.provider.DagTree(ctx, cid)
}

func (e *Engine) DagSize(ctx context.Context, cid string) (string, error) {
	return e.provider.DagSize(ctx, cid)

}

// Pin the given path
func (e *Engine) Pin(path string) error {
	return e.provider.Pin(path)
}

// Unpin the given path
func (e *Engine) Unpin(path string) error {
	return e.provider.Unpin(path)
}

func (e *Engine) List(cid string) ([]*shell.LsLink, error) {
	return e.provider.List(cid)
}

func (e *Engine) RepoStat() (RepoStat, error) {
	return e.provider.RepoStat()
}

func (e *Engine) Start() error {
	return e.provider.Start()
}
