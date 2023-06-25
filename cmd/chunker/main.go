package main

import (
	"context"
	"fmt"
	"net"
	"os"

	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
	"mtcloud.com/mtstorage/api"
	chunkerapi "mtcloud.com/mtstorage/cmd/chunker/api"
	config2 "mtcloud.com/mtstorage/cmd/chunker/config"
	"mtcloud.com/mtstorage/cmd/chunker/nodeimpl"
	"mtcloud.com/mtstorage/cmd/chunker/services"
	"mtcloud.com/mtstorage/node/client"
	"mtcloud.com/mtstorage/pkg/discall"
	xhttp "mtcloud.com/mtstorage/pkg/http"
	"mtcloud.com/mtstorage/pkg/logger"
	"mtcloud.com/mtstorage/pkg/mq"
	utilruntime "mtcloud.com/mtstorage/pkg/runtime"
	"mtcloud.com/mtstorage/pkg/tracing"
	"mtcloud.com/mtstorage/util"
	"mtcloud.com/mtstorage/version"
)

const LocalServiceId = "chunker"

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
	Long:  `Starts chunker service`,
	RunE: func(cmd *cobra.Command, args []string) error {
		if len(args) != 0 {
			return fmt.Errorf("trailing args detected")
		}
		// Parsing of the command line is done so silence cmd usage
		cmd.SilenceUsage = true
		return serve()
	},
}

func serve() error {
	defer utilruntime.HandleCrash()
	c, err := config2.LoadChunkerConfig(LocalServiceId)
	if err != nil {
		logger.Error(err)
		return err
	}

	//init logger
	logger.InitLogger(c.Logger.Level)

	//Set system resources to maximum
	if err := util.SetMaxResources(); err != nil {
		logger.Error(err)
	}
	xhttp.ReqConfigInit(c.Request.Max, c.Request.TimeOut)

	//init trace jaeger
	jaeger := tracing.SetupJaegerTracing("mtoss-chunker", c.Jaeger)
	defer func() {
		if jaeger != nil {
			jaeger.Flush()
		}
	}()

	node := services.NewChunkerNode(c)

	//start mq
	mq.StartMQ(c.Mq, node.NodeGroup, node.Id)

	//register node
	n := nodeimpl.NewChunkerNodeImpl(node)
	discall.CallServer.Register("chunker", n)
	client.InitClient(node.Id, node.Name, "")

	//init nameserver client
	nodeClient, err := client.CreateServerClient(nil, node.NameServerGroup)
	if err != nil {
		panic("nodeId is empty")
	}
	node.NameServer = nodeClient

	//start storage node
	node.Start(c)
	//init api server
	initChunkerRouter(c, node)

	return nil
}

func initChunkerRouter(c *config2.ChunkerConfig, ck *services.Chunker) {
	logger.Info("init chunker router")
	// Initialize router. `SkipClean(true)` stops gorilla/mux from
	router := mux.NewRouter().SkipClean(true).UseEncodedPath()

	// Add healthcheck router
	chunkerapi.RegisterHealthCheckRouter(router)

	//Add server metrics router
	chunkerapi.RegisterMetricsRouter(router)
	// Add API router.
	chunkerapi.RegisterAPIRouter(router, ck)

	// Use all the middlewares
	router.Use(api.GlobalHandlers...)

	ctx := context.Background()
	addr := c.Node.Api
	if addr == "" {
		addr = ":8521"
	}
	httpServer := xhttp.NewServer([]string{addr},
		router, nil)
	httpServer.BaseContext = func(listener net.Listener) context.Context {
		return ctx
	}

	globalHTTPServerErrorCh := make(chan error)

	go func() {
		logger.Infof("starting api Server : %s", addr)
		globalHTTPServerErrorCh <- httpServer.Start()
	}()

	select {
	case <-globalHTTPServerErrorCh:
		//todo: handler signals
		os.Exit(1)
	}
}
