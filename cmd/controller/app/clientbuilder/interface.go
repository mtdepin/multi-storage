package clientbuilder

// client interface
type ControllerClientBuilder interface {
	NameserverClient() *NameserverClient
}
