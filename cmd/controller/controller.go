package main

import (
	"fmt"
	"github.com/fsnotify/fsnotify"
	"github.com/spf13/cobra"
	"k8s.io/apimachinery/pkg/util/wait"
	"mtcloud.com/mtstorage/cmd/controller/app"
	"mtcloud.com/mtstorage/cmd/controller/app/informers/core"
	"mtcloud.com/mtstorage/node/client"
	"mtcloud.com/mtstorage/pkg/config"
	"mtcloud.com/mtstorage/pkg/discall"
	"mtcloud.com/mtstorage/pkg/logger"
	"mtcloud.com/mtstorage/pkg/runtime"
	"mtcloud.com/mtstorage/util"
	"mtcloud.com/mtstorage/version"
	"os"
)

const LocalServiceId = "controller"

var mainCmd = &cobra.Command{Use: LocalServiceId}

func main() {
	mainCmd.AddCommand(version.Cmd())
	mainCmd.AddCommand(startCmd())
	if mainCmd.Execute() != nil {
		os.Exit(1)
	}
}

func startCmd() *cobra.Command {
	return deployStartCmd
}

var deployStartCmd = &cobra.Command{
	Use:   "start",
	Short: "Starts service.",
	Long:  `Starts name server service`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 0 {
			return fmt.Errorf("trailing args detected")
		}
		defer runtime.HandleCrash()

		//init config
		if err := initConfig(LocalServiceId); err != nil {
			return err
		}

		//init logger
		level := config.GetString("logger.level")
		if level == "" {
			level = "info"
		}
		logger.InitLogger(level)

		if err := initServices(); err != nil {
			panic(err)
		}
		// Parsing of the command line is done so silence cmd usage
		cmd.SilenceUsage = true
		if err := app.Run(args, wait.NeverStop); err != nil {
			fmt.Fprintf(os.Stderr, "%v\n", err)
			os.Exit(1)
		}
		//return run(args)
		return nil
	},
}

func initServices() error {

	//register discall node
	n := core.MqW
	discall.CallServer.Register("controller", n)
	client.InitClient("controller", "controller", "")

	//tp := config.GetString("mq.topic")
	//mq.StartMQ(tp, "controller", "testcontroller")
	return nil
}

// InitConfig initializes viper config
func initConfig(cmdRoot string) error {
	configInstance := config.InitViper(LocalServiceId)
	defer func() {
		configInstance.WatchConfig()
		configInstance.Config.OnConfigChange(func(e fsnotify.Event) {
			fmt.Println("配置发生变更：", e.Name)
		})
	}()
	curPath, _ := os.Getwd()
	confPath := curPath + "/conf/"
	configInstance.AddConfigPath(confPath)
	if !util.FileExists(confPath + "controller.yml") {
		fmt.Println(confPath + "chunker.json do not exist")
	}
	configInstance.SetConfigName(LocalServiceId)
	if err := configInstance.ReadInConfig(); err != nil {
		err := fmt.Errorf("storageerror when reading %s.json config file %s", cmdRoot, err)
		panic(err)
	}
	return nil
}
