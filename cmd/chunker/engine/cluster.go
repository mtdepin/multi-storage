package engine

import (
	"context"
	"errors"
	"fmt"
	shell "github.com/ipfs/go-ipfs-api"
	"io"
	"mtcloud.com/mtstorage/pkg/logger"
	"time"
)

type IpfsCluster struct {
	endpoints map[string]*EndpointConfig
}

func newIpfsCluster(c EngineConfig) Storage {
	ic := &IpfsCluster{}
	ic.endpoints = c.Endpoints
	return ic
}

func (c *IpfsCluster) getClient() *shell.Shell {
	length := len(c.endpoints)
	//todo: get a random endpoint,map所有一定随机性
	if length != 0 {
		for host, config := range c.endpoints {
			if !config.CanStore || !config.health {
				continue
			}
			url := fmt.Sprintf("http://%s:%d", host, config.Http)
			e := shell.NewShell(url)
			//logger.Info("cluster上传节点: ", url)
			return e
		}
	}
	err := errors.New("no endpoint found")
	logger.Error(err)

	return nil
}

func (c *IpfsCluster) Start() error {
	c.checkAlive()
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				c.checkAlive()
			}
		}
	}()
	return nil
}

func (c *IpfsCluster) checkAlive() {

	for host, config := range c.endpoints {
		if !config.CanStore {
			continue
		}
		url := fmt.Sprintf("http://%s:%d", host, config.Http)
		e := shell.NewShell(url)
		if !e.IsUp() {
			logger.Errorf("cluster节点连接异常:  %s", url)
			config.health = false
		} else {
			config.health = true
		}
	}
}

func (c *IpfsCluster) Write(ctx context.Context, file io.Reader) (string, error) {
	// 随机选择一个节点。
	cli := c.getClient()
	if cli == nil {
		return "", errors.New("no endpoint found")
	}

	return cli.Add(file, shell.Pin(true))

}

func (c *IpfsCluster) Read(ctx context.Context, cid string) (io.Reader, error) {
	cli := c.getClient()
	if cli == nil {
		return nil, errors.New("no endpoint found")
	}

	resp, err := cli.Request("cat", cid).Send(ctx)
	if err != nil {
		return nil, err
	}
	if resp.Error != nil {
		return nil, resp.Error
	}
	return resp.Output, nil
}

func (c *IpfsCluster) Delete(ctx context.Context, cid string) error {
	cli := c.getClient()
	if cli == nil {
		return errors.New("no endpoint found")
	}

	resp, err := cli.Request("pin/rm", cid).
		Option("recursive", true).
		Send(ctx)
	if err != nil {
		return err
	}
	if resp.Error != nil {
		return resp.Error
	}
	return nil
}

func (c *IpfsCluster) Stat(ctx context.Context, cid string) error {
	cli := c.getClient()
	if cli == nil {
		return errors.New("no endpoint found")
	}

	var raw = struct {
		Key  string
		Size uint64
	}{}
	err := cli.Request("block/stat", cid).Exec(ctx, &raw)
	if err != nil {
		return err
	}

	if raw.Size <= 0 {
		return errors.New("block size unexpected")
	}
	return nil
}

func (c *IpfsCluster) DagTree(ctx context.Context, cid string) (interface{}, error) {
	cli := c.getClient()
	if cli == nil {
		return nil, errors.New("no endpoint found")
	}
	var res interface{}
	if err := cli.DagGet(cid, &res); err != nil {
		logger.Error("get dag fail: ", err)
		return nil, err
	}
	return res, nil
}

func (c *IpfsCluster) DagSize(ctx context.Context, cid string) (string, error) {

	cli := c.getClient()
	if cli == nil {
		return "", errors.New("no endpoint found")
	}
	ObjectStats, err := cli.ObjectStat(cid)
	if err != nil {
		logger.Error("get dag size fail: ", err)
		return "", err
	}
	return fmt.Sprintf("%d", ObjectStats.CumulativeSize), nil
}

// Pin the given path
func (c *IpfsCluster) Pin(path string) error {
	cli := c.getClient()
	if cli == nil {
		return errors.New("no endpoint found")
	}
	return cli.Pin(path)
}

// Unpin the given path
func (c *IpfsCluster) Unpin(path string) error {
	cli := c.getClient()
	if cli == nil {
		return errors.New("no endpoint found")
	}
	return cli.Unpin(path)
}

func (c *IpfsCluster) List(cid string) ([]*shell.LsLink, error) {
	cli := c.getClient()
	if cli == nil {
		return nil, errors.New("no endpoint found")
	}
	return cli.List(cid)
}

func (c *IpfsCluster) RepoStat() (RepoStat, error) {
	cli := c.getClient()
	if cli == nil {
		return RepoStat{}, errors.New("no endpoint found")
	}
	var stat RepoStat
	err := cli.Request("repo/stat").Option("size-only", "true").Exec(context.Background(), &stat)
	return stat, err
}
