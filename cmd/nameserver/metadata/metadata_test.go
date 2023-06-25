package metadata

import (
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/fsnotify/fsnotify"
	"mtcloud.com/mtstorage/pkg/config"
	"mtcloud.com/mtstorage/pkg/logger"
	"mtcloud.com/mtstorage/util"
)

// InitConfig initializes viper config
func setup() error {
	var LocalServiceId = "nameserver"
	configInstance := config.InitViper(LocalServiceId)
	defer func() {
		configInstance.WatchConfig()
		configInstance.Config.OnConfigChange(func(e fsnotify.Event) {
			fmt.Println("配置发生变更：", e.Name)
		})
	}()
	curPath, _ := os.Getwd()
	confPath := curPath + "/../../../conf/"
	configInstance.AddConfigPath(confPath)
	if !util.FileExists(confPath + "nameserver.json") {
		fmt.Println(confPath + "nameserver.json do not exist")
		return errors.New("config file not exist")
	}
	configInstance.SetConfigName(LocalServiceId)
	if err := configInstance.ReadInConfig(); err != nil {
		logger.Errorf("storageerror when reading config file err: %s", err)
		return errors.New("read config failed")
	}
	logger.InitLogger("info")
	InitMetadata()
	return nil
}

func TestInitMetadata(t *testing.T) {
	err := setup()
	if err != nil {
		t.Fatalf("init metadata failed : %s", err)
	}
}
