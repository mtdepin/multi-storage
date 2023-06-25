package main

import (
	"context"
	"fmt"
	"mtcloud.com/mtstorage/cmd/nameserver/backend"
	config2 "mtcloud.com/mtstorage/cmd/nameserver/config"
	"mtcloud.com/mtstorage/cmd/nameserver/nodeimpl"
	"mtcloud.com/mtstorage/node/client"
	"mtcloud.com/mtstorage/pkg/discall"
	"mtcloud.com/mtstorage/pkg/mq"
	"net"
	"net/http"
	"net/http/pprof"
	"os"

	"github.com/gorilla/mux"
	"github.com/spf13/cobra"
	httpSwagger "github.com/swaggo/http-swagger"
	"mtcloud.com/mtstorage/api"
	"mtcloud.com/mtstorage/cmd/nameserver/httpapi"
	"mtcloud.com/mtstorage/cmd/nameserver/metadata"
	"mtcloud.com/mtstorage/pkg/cache"
	"mtcloud.com/mtstorage/pkg/crypto"
	xhttp "mtcloud.com/mtstorage/pkg/http"
	"mtcloud.com/mtstorage/pkg/lock"
	"mtcloud.com/mtstorage/pkg/logger"
	utilruntime "mtcloud.com/mtstorage/pkg/runtime"
	"mtcloud.com/mtstorage/pkg/tracing"
	"mtcloud.com/mtstorage/util"
	"mtcloud.com/mtstorage/version"
)

const LocalServiceId = "nameserver"

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
		// Parsing of the command line is done so silence cmd usage
		cmd.SilenceUsage = true
		return startServer(args)
	},
}

func startServer(args []string) error {

	defer utilruntime.HandleCrash()
	c, err := config2.LoadNameServerConfig(LocalServiceId)
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
	jaeger := tracing.SetupJaegerTracing("mtoss-nameserver", c.Jaeger)
	defer func() {
		if jaeger != nil {
			jaeger.Flush()
		}
	}()

	//init handler of distributed locker
	redisaddr := c.Redis.Url
	redispwd := crypto.DecryptLocalPassword(c.Redis.Password)
	lock.Init(redisaddr, redispwd)
	cache.Init(redisaddr, redispwd)

	//init storage node
	ns := backend.NewNameServer(c)

	//init mq
	mq.StartMQ(c.Mq, ns.NodeGroup, ns.Id)

	//register nameserver rpc api
	n := nodeimpl.NewServerNodeImpl(ns)
	discall.CallServer.Register("nameserver", n)
	client.InitClient(ns.Id, ns.Name, "")
	controlNode := nodeimpl.NewControlNodeImpl(ns)
	discall.CallServer.Register("control-node", controlNode)
	client.InitClient(ns.Id, ns.Name, "")

	//init controller rpc client
	nodeClient, err := client.CreateControllerlient(nil, "controller")
	if err != nil {
		panic(err)
	}
	ns.Controller = nodeClient

	//init metadata module
	metadata.InitMetadata(c.DB)

	ns.Start()

	//init http api server
	initNameserverRouter(c, ns)

	return nil
}

func initNameserverRouter(c *config2.NameServerConfig, ns *backend.NameServer) {
	logger.Info("init nameserver router")

	// Initialize router. `SkipClean(true)` stops gorilla/mux from
	router := mux.NewRouter().SkipClean(true).UseEncodedPath()

	// Add healthcheck router
	httpapi.RegisterHealthCheckRouter(router)

	//Add server metrics router
	httpapi.RegisterMetricsRouter(router)

	// Add API router.
	httpapi.RegisterAPIRouter(router, ns)

	// Use all the middlewares
	router.Use(api.GlobalHandlers...)
	//register swagger
	router.PathPrefix("/swagger").Handler(httpSwagger.WrapHandler)

	ctx, _ := context.WithCancel(context.Background())

	httpServer := xhttp.NewServer([]string{c.Node.Api},
		router, nil)
	httpServer.BaseContext = func(listener net.Listener) context.Context {
		return ctx
	}

	globalHTTPServerErrorCh := make(chan error)

	go func() {
		logger.Infof("starting api Server : %s", c.Node.Api)
		globalHTTPServerErrorCh <- httpServer.Start()
	}()

	if c.Profile.Enable {
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/debug/pprof/", pprof.Index)
			mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
			http.ListenAndServe("0.0.0.0:18500", mux)
		}()

	}

	for {
		select {
		case <-globalHTTPServerErrorCh:
			//todo: handler signals
			os.Exit(1)
		}
	}
}
