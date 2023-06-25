package config

import (
	"errors"
	"fmt"
	"github.com/fsnotify/fsnotify"
	"mtcloud.com/mtstorage/cmd/chunker/engine"
	"mtcloud.com/mtstorage/pkg/config"
	"mtcloud.com/mtstorage/pkg/db"
	"mtcloud.com/mtstorage/pkg/logger"
	"mtcloud.com/mtstorage/util"
	"os"
)

type NameServerConfig struct {
	Node    NodeConfig
	Mq      config.MqConfig
	Storage engine.StorageConfig
	Logger  config.LoggerConfig
	Jaeger  config.JaegerConfig
	Request config.RequestConfig
	DB      db.DBconfig
	Redis   config.RedisConfig
	Profile config.ProfileConfig
}

type NodeConfig struct {
	config.CommonNodeConfig
	Api string
}

func LoadNameServerConfig(serviceId string) (*NameServerConfig, error) {
	if err := initConfig(serviceId, ""); err != nil {
		return nil, err
	}
	var c NameServerConfig
	err := config.Unmarshal(&c)
	if err != nil {
		return nil, err
	}

	//todo： 由于上面不能解析出一些字段，这里单独解析通用node配置，
	var cc config.CommonNodeConfig
	err = config.UnmarshalKey("node", &cc)
	if err != nil {
		return nil, err
	}
	c.Node.CommonNodeConfig = cc

	if err := c.checkConfig(); err != nil {
		return nil, err
	}
	return &c, nil
}

func initConfig(serviceId string, cmdRoot string) error {
	configInstance := config.InitViper(serviceId)
	defer func() {
		configInstance.WatchConfig()
		configInstance.Config.OnConfigChange(func(e fsnotify.Event) {
			fmt.Println("配置发生变更：", e.Name)
		})
	}()
	curPath, _ := os.Getwd()
	confPath := curPath + "/conf/"
	configInstance.AddConfigPath(confPath)
	if !util.FileExists(confPath + "nameserver.yml") {
		return errors.New(confPath + "nameserver.yml do not exist")
	}
	configInstance.SetConfigName(serviceId)
	if err := configInstance.ReadInConfig(); err != nil {
		err := fmt.Errorf("storageerror when reading %s.json config file %s", cmdRoot, err)
		logger.Error(err)
		return err
	}
	return nil
}

func (c *NameServerConfig) checkConfig() error {
	if c.Node.Node_group == "" {
		c.Node.Node_group = "ns_group"
	}

	if c.Node.Region == "" {
		c.Node.Region = "cd"
	}

	if c.Node.Api == "" {
		c.Node.Api = "0.0.0.0:8522"
	}

	if c.Mq.Server == "" {
		c.Mq.Server = " 127.0.0.1:9876"
	}

	if c.Mq.Topic == "" {
		c.Mq.Topic = " rpc_default"
	}

	if c.Mq.Topic_heartBeat == "" {
		c.Mq.Topic_heartBeat = " heartbeat_default"
	}

	if c.Logger.Level == "" {
		c.Logger.Level = "info"
	}

	if c.Request.Max == 0 {
		c.Request.Max = 1024
	}

	if c.Request.TimeOut == 0 {
		c.Request.Max = 120
	}

	if c.DB.Url == "" {
		return errors.New("check config fail: database url not found")
	}

	if c.Redis.Url == "" {
		return errors.New("check config fail: redis url not found")
	}

	return nil

}
