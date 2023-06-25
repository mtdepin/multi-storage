package replication

import (
	"mtcloud.com/mtstorage/cmd/controller/app/clientbuilder"
	"mtcloud.com/mtstorage/pkg/runtime"
)

// Controller manages selector-based service bucket.
type Controller struct {
	nameserverClient *clientbuilder.NameserverClient
}

// NewReplicationController returns a new *Controller.
func NewReplicationController(nscli *clientbuilder.NameserverClient) *Controller {

	c := &Controller{
		nameserverClient: nscli,
	}
	return c
}

func (c *Controller) Run(workers int, stopCh <-chan struct{}) {

	go func() {
		defer runtime.HandleCrash()
	}()

	//todo: start work

	<-stopCh
}
