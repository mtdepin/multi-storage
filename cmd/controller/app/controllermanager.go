package app

import (
	"context"
	"fmt"
	"os"
	"time"

	"mtcloud.com/mtstorage/cmd/controller/app/controller/ipfs"
	"mtcloud.com/mtstorage/cmd/controller/app/informers/core"
	"mtcloud.com/mtstorage/node/client"
	"mtcloud.com/mtstorage/pkg/crypto"
	"mtcloud.com/mtstorage/pkg/discall"
	"mtcloud.com/mtstorage/pkg/mq"

	"mtcloud.com/mtstorage/cmd/controller/app/controller/replication"

	"mtcloud.com/mtstorage/cmd/controller/app/clientbuilder"
	"mtcloud.com/mtstorage/cmd/controller/app/controller/bucket"
	"mtcloud.com/mtstorage/cmd/controller/app/controller/logarchive"
	"mtcloud.com/mtstorage/cmd/controller/app/informers"
	"mtcloud.com/mtstorage/pkg/config"
	"mtcloud.com/mtstorage/pkg/lock"
	"mtcloud.com/mtstorage/pkg/logger"
	utilruntime "mtcloud.com/mtstorage/pkg/runtime"
	"mtcloud.com/mtstorage/pkg/tools/leaderelection"
	"mtcloud.com/mtstorage/pkg/tools/leaderelection/resourcelock"
	"mtcloud.com/mtstorage/version"
)

type InitFunc func(ctx context.Context, controllerCtx ControllerContext) (controller Interface, enabled bool, err error)

// ControllerInitializersFunc is used to create a collection of initializers
//
//	given the loopMode.
type ControllerInitializersFunc func() (initializers map[string]InitFunc)

func Run(args []string, stopCh <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	fmt.Printf("%+v", version.GetInfo())

	//init handler of distributed locker
	redisaddr := config.GetString("redis.url")
	redispwd := crypto.DecryptLocalPassword(config.GetString("redis.password"))
	if redisaddr != "" {
		lock.Init(redisaddr, redispwd)
	}

	run := func(ctx context.Context, startSATokenController InitFunc, initializersFunc ControllerInitializersFunc) {
		//client builder
		clientBuilder := clientbuilder.CreateControllerClientBuilder()
		//clientBuilder := clientbuilder.SimpleControllerClientBuilder{}

		//controller context
		controllerContext, err := CreateControllerContext(&clientBuilder, ctx.Done())
		if err != nil {
			logger.Fatalf("error building controller context: %v", err)
		}

		//init controller
		controllerInitializers := initializersFunc()
		if err := StartControllers(ctx, controllerContext, controllerInitializers, nil, nil); err != nil {
			logger.Fatalf("error starting controllers: %v", err)
		}

		//start informer factory
		controllerContext.InformerFactory.Start(stopCh)
		close(controllerContext.InformersStarted)

		err = initServices()
		if err != nil {
			panic(err)
		}

		//wait stop
		select {}
	}

	//start leader election
	go leaderElectAndRun("controller", leaderelection.LeaderCallbacks{
		OnStartedLeading: func(ctx context.Context) {
			initializersFunc := NewControllerInitializers
			//init controller
			run(ctx, nil, initializersFunc)
		},
		OnStoppedLeading: func() {
			panic("leaderelection lost")
			os.Exit(1)
		},
	})

	select {}
	return nil

}

func initServices() error {

	//register discall node
	n := core.MqW
	discall.CallServer.Register("controller", n)
	client.InitClient("controller", "controller", "")

	tp := config.GetString("mq.topic")
	mq.StartMQ(tp, "controller", "testcontroller")
	return nil
}

// leaderElectAndRun runs the leader election, and runs the callbacks once the leader lease is acquired.
// TODO: extract this function into staging/controller-app
func leaderElectAndRun(leaseName string, callbacks leaderelection.LeaderCallbacks) {
	rl, err := resourcelock.New(resourcelock.LeasesResourceLock, "", "", nil, resourcelock.ResourceLockConfig{
		Identity: config.GetString("node.id"),
	})
	if err != nil {
		logger.Fatalf("error creating lock: %v", err)
	}

	leaderelection.RunOrDie(context.TODO(), leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: 30 * time.Second,
		RenewDeadline: 15 * time.Second,
		RetryPeriod:   5 * time.Second,
		Callbacks:     callbacks,
		WatchDog:      nil,
		Name:          leaseName,
	})

	panic("unreachable")
}

// NewControllerInitializers is a public map of named controller groups (you can start more than one in an init func)
// paired to their InitFunc.  This allows for structured downstream composition and subdivision.
func NewControllerInitializers() map[string]InitFunc {
	controllers := map[string]InitFunc{}
	controllers["bucket"] = startBucketController
	controllers["bucketLogArchive"] = startBucketLogArchiveController
	controllers["replication"] = startReplicationController
	controllers["IpfsCidAnalysis"] = startIpfsCidAnalysisController

	return controllers

}

func StartControllers(ctx context.Context, controllerCtx ControllerContext, controllers map[string]InitFunc, unsecuredMux *interface{}, healthzHandler *interface{}) error {
	for controllerName, initFn := range controllers {
		time.Sleep(5 * time.Second)

		logger.Infof("Starting %q", controllerName)
		_, started, err := initFn(ctx, controllerCtx)
		if err != nil {
			logger.Errorf("Error starting %q", controllerName)
			return err
		}
		if !started {
			logger.Warnf("Skipping %q", controllerName)
			continue
		}

		logger.Infof("Started %q", controllerName)
	}

	logger.Info("[OK] all controller start complete!")

	return nil
}

// ControllerContext defines the context object for controller
type ControllerContext struct {
	ClientBuilder clientbuilder.ControllerClientBuilder

	// ComponentConfig provides access to init options for a given controller
	ComponentConfig ComponentConfig

	// InformerFactory gives access to informers for the controller.
	InformerFactory informers.SharedInformerFactory

	// InformersStarted is closed after all of the controllers have been initialized and are running.  After this point it is safe,
	// for an individual controller to start the shared informers. Before it is closed, they should not.
	InformersStarted chan struct{}

	// ResyncPeriod generates a duration each time it is invoked; this is so that
	// multiple controllers don't get into lock-step and all hammer the apiserver
	// with list requests simultaneously.
	ResyncPeriod func() time.Duration
}

// CreateControllerContext creates a context struct containing references to resources needed by the
// controllers
func CreateControllerContext(clientBuilder *clientbuilder.SimpleControllerClientBuilder, stop <-chan struct{}) (ControllerContext, error) {
	nameserverClient := clientBuilder.NameserverClient()

	//todo resync time
	sharedInformers := informers.NewSharedInformerFactory(nameserverClient, 1*time.Minute)

	ctx := ControllerContext{
		ClientBuilder:    clientBuilder,
		InformerFactory:  sharedInformers,
		InformersStarted: make(chan struct{}),
		//todo resync time
		ResyncPeriod: func() time.Duration {
			return 2 * time.Minute
		},
	}
	return ctx, nil
}

func startBucketController(ctx context.Context, controllerCtx ControllerContext) (controller Interface, enabled bool, err error) {
	logger.Infof("start bucket controller")
	go bucket.NewBucketController(
		controllerCtx.InformerFactory.Core().Buckets(),
		controllerCtx.ClientBuilder.NameserverClient(),
		5*time.Second,
	).Run(3, ctx.Done())
	return nil, true, nil
}

func startBucketLogArchiveController(ctx context.Context, controllerCtx ControllerContext) (controller Interface, enabled bool, err error) {
	logger.Info("start bucket logging archive controller")
	go logarchive.NewLoggingController(
		controllerCtx.ClientBuilder.NameserverClient(),
	).Run(3, ctx.Done())

	return nil, true, nil
}

func startReplicationController(ctx context.Context, controllerCtx ControllerContext) (controller Interface, enabled bool, err error) {
	logger.Info("start replication controller")
	go replication.NewReplicationController(
		controllerCtx.ClientBuilder.NameserverClient(),
	).Run(3, ctx.Done())

	return nil, true, nil
}

func startIpfsCidAnalysisController(ctx context.Context, controllerCtx ControllerContext) (controller Interface, enabled bool, err error) {
	logger.Info("start ipfsCidAnalysis controller")
	go ipfs.NewIpfsCidAnalysisController(
		controllerCtx.InformerFactory.Core().Objects(),
		controllerCtx.ClientBuilder.NameserverClient(),
	).Run(3, ctx.Done())

	return nil, true, nil
}
