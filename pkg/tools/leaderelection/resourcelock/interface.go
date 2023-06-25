package resourcelock

import (
	"context"
	"fmt"
	"time"
)

const (
	LeaderElectionRecordAnnotationKey = "control-plane.alpha.kubernetes.io/leader"
	EndpointsResourceLock             = "endpoints"
	ConfigMapsResourceLock            = "configmaps"
	LeasesResourceLock                = "leases"
	EndpointsLeasesResourceLock       = "endpointsleases"
	ConfigMapsLeasesResourceLock      = "configmapsleases"
)

// LeaderElectionRecord is the record that is stored in the leader election annotation.
// This information should be used for observational purposes only and could be replaced
// with a random string (e.g. UUID) with only slight modification of this code.
// TODO(mikedanese): this should potentially be versioned
type LeaderElectionRecord struct {
	// HolderIdentity is the ID that owns the lease. If empty, no one owns this lease and
	// all callers may acquire. Versions of this library prior to Kubernetes 1.14 will not
	// attempt to acquire leases with empty identities and will wait for the full lease
	// interval to expire before attempting to reacquire. This value is set to empty when
	// a client voluntarily steps down.
	HolderIdentity       string    `json:"holderIdentity"`
	LeaseDurationSeconds int       `json:"leaseDurationSeconds"`
	AcquireTime          time.Time `json:"acquireTime"`
	RenewTime            time.Time `json:"renewTime"`
	LeaderTransitions    int       `json:"leaderTransitions"`
}

// EventRecorder records a change in the ResourceLock.
type EventRecorder interface {
	//Eventf(obj runtime.Object, eventType, reason, message string, args ...interface{})
}

// ResourceLockConfig common data that exists across different
// resource locks
type ResourceLockConfig struct {
	// Identity is the unique string identifying a lease holder across
	// all participants in an election.
	Identity string
	// EventRecorder is optional.
	EventRecorder EventRecorder
}

// Interface offers a common interface for locking on arbitrary
// resources used in leader election.  The Interface is used
// to hide the details on specific implementations in order to allow
// them to change over time.  This interface is strictly for use
// by the leaderelection code.
type Interface interface {
	// Get returns the LeaderElectionRecord
	Get(ctx context.Context) (*LeaderElectionRecord, []byte, error)

	// Create attempts to create a LeaderElectionRecord
	Create(ctx context.Context, ler LeaderElectionRecord) error

	// Update will update and existing LeaderElectionRecord
	Update(ctx context.Context, ler LeaderElectionRecord) error

	// RecordEvent is used to record events
	RecordEvent(string)

	// Identity will return the locks Identity
	Identity() string

	// Describe is used to convert details on current resource lock
	// into a string
	Describe() string
}

// Manufacture will create a lock of a given type according to the input parameters
func New(lockType string, ns string, name string, coordinationClient interface{}, rlc ResourceLockConfig) (Interface, error) {
	leaseLock := &LeaseLock{
		LeaseMeta:  nil,
		Client:     coordinationClient,
		LockConfig: rlc,
	}
	switch lockType {
	case LeasesResourceLock:
		return leaseLock, nil
	default:
		return nil, fmt.Errorf("Invalid lock-type %s", lockType)
	}
}
