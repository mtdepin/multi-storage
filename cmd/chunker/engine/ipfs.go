package engine

import (
	"context"
	"errors"
	"fmt"
	shell "github.com/ipfs/go-ipfs-api"
	"go.opencensus.io/trace"
	"io"
	"mtcloud.com/mtstorage/pkg/logger"
	"sync"
	"time"
)

type Ipfs struct {
	replication int
	endpoints   map[string]*EndpointConfig
}

func newIpfs(c StorageConfig) Storage {
	ic := &Ipfs{
		replication: c.Replication,
	}
	ic.endpoints = c.Targets[STORAGE_IPFS].Endpoints
	return ic
}

func (c *Ipfs) getClient() *shell.Shell {
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

func (c *Ipfs) Write(ctx context.Context, file io.Reader) (string, error) {
	// 随机选择一个节点。
	if c.replication == 1 {
		cli := c.getClient()
		if cli == nil {
			return "", errors.New("no endpoint found")
		}
		return cli.Add(file, shell.Pin(true))
	}

	return c.MultiStream(ctx, file)

}

func (c *Ipfs) allocate() ([]*shell.Shell, error) {
	var eps []*shell.Shell
	if c.replication == -1 {
		for host, config := range c.endpoints {

			if !config.CanStore || !config.health {
				continue
			}
			url := fmt.Sprintf("http://%s:%d", host, config.Http)
			cli := shell.NewShell(url)
			//logger.Info("cluster上传节点: ", url)
			eps = append(eps, cli)
		}
		return eps, nil
	}

	//寻找与replication一致的节点数
	for host, config := range c.endpoints {
		if !config.CanStore || !config.health {
			continue
		}
		url := fmt.Sprintf("http://%s:%d", host, config.Http)
		cli := shell.NewShell(url)
		//logger.Info("cluster上传节点: ", url)
		eps = append(eps, cli)
		if len(eps) == c.replication {
			break
		}
	}

	if len(eps) != c.replication {
		err := fmt.Errorf("not enough node to allocate, need: %d, have: %d", c.replication, len(eps))
		return eps, err
	}

	return eps, nil
}

func (c *Ipfs) MultiStream(ctx context.Context, file io.Reader) (cid string, err error) {
	ctx, span := trace.StartSpan(ctx, "writeDataToMutIPFS")
	defer span.End()

	eps, err := c.allocate()
	if err != nil {
		logger.Error(err)
		return "", err
	}

	var sm sync.Map
	var wg sync.WaitGroup
	length := len(eps)
	var w = make([]io.Writer, length, length)
	var pr = make([]*io.PipeReader, length, length)
	ch := make(chan struct{}, length)
	for i := 0; i < length; i++ {
		wg.Add(1)
		go func(index int) {
			pr[index], w[index] = io.Pipe()
			ch <- struct{}{}
			cli := eps[index]
			cid, err := cli.Add(pr[index], shell.Pin(true))
			sm.Store(cid, err)
			if err != nil {
				if pw, ok := w[index].(*io.PipeWriter); ok {
					pw.CloseWithError(err)
				}
				id, _ := cli.ID()
				logger.Error("ipfs节点--》", id, "  上传失败", err)
			}
			wg.Done()
		}(i)
	}
	// 写入通道全部启动才开始copy
	for {
		if len(ch) == length {
			break
		}
	}
	mw := io.MultiWriter(w...) // 向mw统一写入
	_, err = io.Copy(mw, file)
	if err != nil {
		return "", err
	}
	for i := 0; i < length; i++ {
		if pw, ok := w[i].(*io.PipeWriter); ok {
			pw.CloseWithError(err)
		}
	}
	wg.Wait()
	sm.Range(func(key, value interface{}) bool {
		if value != nil {
			err = value.(error)
			return false
		}
		if cid != "" && cid != key.(string) {
			err = errors.New("cid不一致出现错误！")
			return false
		}
		cid = key.(string)
		return true
	})
	if cid == "" {
		return "", errors.New("上传失败！")
	}
	return cid, err
}

func (c *Ipfs) Read(ctx context.Context, cid string) (io.Reader, error) {
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

func (c *Ipfs) Delete(ctx context.Context, cid string) error {
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

func (c *Ipfs) Stat(ctx context.Context, cid string) error {
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

func (c *Ipfs) DagTree(ctx context.Context, cid string) (interface{}, error) {
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

func (c *Ipfs) DagSize(ctx context.Context, cid string) (string, error) {

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
func (c *Ipfs) Pin(path string) error {
	cli := c.getClient()
	if cli == nil {
		return errors.New("no endpoint found")
	}
	return cli.Pin(path)
}

func (c *Ipfs) RepoStat() (RepoStat, error) {
	//todo :多个ipfs节点情况下的状态收集
	//get repo stat
	cli := c.getClient()
	if cli == nil {
		return RepoStat{}, errors.New("no endpoint found")
	}
	var stat RepoStat
	cli.SetTimeout(5 * time.Second)
	err := cli.Request("repo/stat").Option("size-only", "true").Exec(context.Background(), &stat)
	return stat, err
}

func (c *Ipfs) Start() error {
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

func (c *Ipfs) checkAlive() {

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

func (c *Ipfs) Unpin(path string) error {
	cli := c.getClient()
	if cli == nil {
		return errors.New("no endpoint found")
	}
	return cli.Unpin(path)
}

func (c *Ipfs) List(cid string) ([]*shell.LsLink, error) {
	cli := c.getClient()
	if cli == nil {
		return nil, errors.New("no endpoint found")
	}
	return cli.List(cid)
}
