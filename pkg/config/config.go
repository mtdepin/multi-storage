package config

import (
	"fmt"
	"github.com/spf13/viper"
	"io"
	"io/ioutil"
	"mtcloud.com/mtstorage/pkg/logger"
	"os"
	"strings"
)

var disConfig *DisConfig

// DisConfig local yml yaml in local
type DisConfig struct {
	Config *viper.Viper
}

// ConfigInstance get instance  of config
func ConfigInstance() *DisConfig {
	return disConfig
}

func InitViper(LocalServiceId string) *DisConfig {
	cViper := viper.NewWithOptions(viper.KeyDelimiter(":"))
	cViper.SetEnvPrefix(LocalServiceId)
	cViper.AutomaticEnv()
	cViper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	disConfig = &DisConfig{
		Config: cViper,
	}
	return disConfig
}

// WatchConfig
func (config *DisConfig) WatchConfig() {
	config.Config.WatchConfig()
}

// SetConfigName
func (config *DisConfig) SetConfigName(in string) {
	config.Config.SetConfigName(in)
}

// SetEnvironment set viper in config
func (config *DisConfig) AddConfigPath(confPath string) {
	config.Config.AddConfigPath(confPath)
}

// EvnChanged will merge
func (config *DisConfig) EvnChanged(in io.Reader) {
	config.Config.MergeConfig(in)
}

// EvnChanged will merge
func (config *DisConfig) ReadInConfig() error {
	return config.Config.ReadInConfig()
}

// ReadLocationConfig get config content
func (config *DisConfig) ReadLocationConfig(defaultPath string) (error, string) {
	nacosFile, err := ioutil.ReadFile(defaultPath)
	if err != nil {
		logger.Info(err.Error())
		return err, ""
	}
	readContent := string(nacosFile)
	for _, value := range os.Environ() {
		keyValue := strings.Split(value, "=")
		if len(keyValue) == 2 {
			find := fmt.Sprintf("${%s}", keyValue[0])
			if strings.Index(readContent, find) > 0 {
				readContent = strings.ReplaceAll(readContent, find, keyValue[1])
			}
		}
	}
	return nil, readContent
}

func GetNodeGroup() string {
	tp := GetString("mq.topic")
	if tp == "" {
		panic("name is empty")
	}

	ng := GetString("node.node_group")
	if ng == "" {
		panic("group is empty")
	}

	return fmt.Sprintf("%s#%s", tp, ng)
}

func GetString(key string) string {
	ins := ConfigInstance()
	return ins.Config.GetString(key)
}

func UnmarshalKey(key string, rawVal interface{}) error {
	ins := ConfigInstance()
	return ins.Config.UnmarshalKey(key, rawVal)
}

func Unmarshal(rawVal interface{}) error {
	ins := ConfigInstance()
	return ins.Config.Unmarshal(rawVal)
}

func GetInt(key string) int {
	ins := ConfigInstance()
	return ins.Config.GetInt(key)
}

func GetBool(key string) bool {
	ins := ConfigInstance()
	return ins.Config.GetBool(key)
}

func GetMap(key string) map[string]interface{} {
	ins := ConfigInstance()
	return ins.Config.GetStringMap(key)
}
