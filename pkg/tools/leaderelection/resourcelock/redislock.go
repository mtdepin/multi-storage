package resourcelock

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/go-redis/redis/v8"
	"mtcloud.com/mtstorage/pkg/lock"
	"time"
)

var (
	luaRefresh = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pexpire", KEYS[1], ARGV[2]) else return 0 end`)
	luaRelease = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("del", KEYS[1]) else return 0 end`)
	luaPTTL    = redis.NewScript(`if redis.call("get", KEYS[1]) == ARGV[1] then return redis.call("pttl", KEYS[1]) else return -3 end`)
)

const (
	electionKey = "leader-election-key"
)

type LeaseLock struct {
	// LeaseMeta should contain a Name and a Namespace of a
	// LeaseMeta object that the LeaderElector will attempt to lead.
	LeaseMeta interface{}
	//Client     coordinationv1client.LeasesGetter
	Client     interface{}
	LockConfig ResourceLockConfig
	lease      []byte
	//lease      *coordinationv1.Lease
}

// LeaseSpec is a specification of a Lease.
type LeaseSpec struct {
	// holderIdentity contains the identity of the holder of a current lease.
	// +optional
	HolderIdentity *string `json:"holderIdentity,omitempty" protobuf:"bytes,1,opt,name=holderIdentity"`
	// leaseDurationSeconds is a duration that candidates for a lease need
	// to wait to force acquire it. This is measure against time of last
	// observed RenewTime.
	// +optional
	LeaseDurationSeconds *int32 `json:"leaseDurationSeconds,omitempty" protobuf:"varint,2,opt,name=leaseDurationSeconds"`
	// acquireTime is a time when the current lease was acquired.
	// +optional
	AcquireTime time.Time `json:"acquireTime,omitempty" protobuf:"bytes,3,opt,name=acquireTime"`
	// renewTime is a time when the current holder of a lease has last
	// updated the lease.
	// +optional
	RenewTime time.Time `json:"renewTime,omitempty" protobuf:"bytes,4,opt,name=renewTime"`
	// leaseTransitions is the number of transitions of a lease between
	// holders.
	// +optional
	LeaseTransitions *int32 `json:"leaseTransitions,omitempty" protobuf:"varint,5,opt,name=leaseTransitions"`
}

// Get returns the election record from a Lease spec
func (ll *LeaseLock) Get(ctx context.Context) (*LeaderElectionRecord, []byte, error) {
	var err error
	//recordByte, err := []byte("abcd"), nil
	cli := lock.GlobalRedisHandler.Cli

	recordByte, err := cli.Get(ctx, electionKey).Bytes()
	if err != nil {
		return nil, nil, err
	}
	ll.lease = recordByte
	//record := LeaseSpecToLeaderElectionRecord(&ll.lease)
	var record LeaderElectionRecord
	err = json.Unmarshal(recordByte, &record)
	if err != nil {
		return nil, nil, err
	}
	return &record, recordByte, nil
	//return nil, nil, nil
}

// Create attempts to create a Lease
func (ll *LeaseLock) Create(ctx context.Context, ler LeaderElectionRecord) error {
	var err error
	//ll.lease, err = ll.Client.Leases(ll.LeaseMeta.Namespace).Create(ctx, &coordinationv1.Lease{
	//	ObjectMeta: metav1.ObjectMeta{
	//		Name:      ll.LeaseMeta.Name,
	//		Namespace: ll.LeaseMeta.Namespace,
	//	},
	//	Spec: LeaderElectionRecordToLeaseSpec(&ler),
	//}, metav1.CreateOptions{})

	recordBytes, err := json.Marshal(&ler)
	cli := lock.GlobalRedisHandler.Cli

	ok, err := cli.SetNX(ctx, electionKey, recordBytes, time.Duration(ler.LeaseDurationSeconds)*time.Second).Result()
	if err != nil {
		return err
	} else if !ok {
		return errors.New("unexpected error!")
	}
	return nil
}

// Update will update an existing Lease spec.
func (ll *LeaseLock) Update(ctx context.Context, ler LeaderElectionRecord) error {
	//if ll.lease == nil {
	//	return errors.New("lease not initialized, call get or create first")
	//}
	//ll.lease.Spec = LeaderElectionRecordToLeaseSpec(&ler)
	//
	//lease, err := ll.Client.Leases(ll.LeaseMeta.Namespace).Update(ctx, ll.lease, metav1.UpdateOptions{})
	//if err != nil {
	//	return err
	//}

	recordBytes, err := json.Marshal(&ler)
	cli := lock.GlobalRedisHandler.Cli
	status, err := luaRefresh.Run(ctx, cli, []string{electionKey}, recordBytes, ler.LeaseDurationSeconds).Result()
	if err != nil {
		return err
	} else if status == int64(1) {
		return nil
	}

	ll.lease = recordBytes

	return nil
}

// RecordEvent in leader election while adding meta-data
func (ll *LeaseLock) RecordEvent(s string) {
	//if ll.LockConfig.EventRecorder == nil {
	//	return
	//}
	//events := fmt.Sprintf("%v %v", ll.LockConfig.Identity, s)
	//ll.LockConfig.EventRecorder.Eventf(&coordinationv1.Lease{ObjectMeta: ll.lease.ObjectMeta}, corev1.EventTypeNormal, "LeaderElection", events)
}

// Describe is used to convert details on current resource lock
// into a string
func (ll *LeaseLock) Describe() string {
	//return fmt.Sprintf("%v/%v", ll.LeaseMeta.Namespace, ll.LeaseMeta.Name)
	return ""
}

// Identity returns the Identity of the lock
func (ll *LeaseLock) Identity() string {
	return ll.LockConfig.Identity
}
