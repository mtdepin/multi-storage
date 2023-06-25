package http

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"testing"
	"time"
)

var (
	testFreq                 = 1 * time.Second
	testDefaultLookupTimeout = 1 * time.Second
)

func logOnce(ctx context.Context, err error, id interface{}, errKind ...interface{}) {
	// no-op
}

func testDNSCache(t *testing.T) *DNSCache {
	t.Helper() // skip printing file and line information from this function
	return NewDNSCache(testFreq, testDefaultLookupTimeout, logOnce)
}

func TestDialContextWithDNSCache(t *testing.T) {
	resolver := &DNSCache{}
	resolver.cache.Store("play.min.io", []string{
		"127.0.0.1",
		"127.0.0.2",
		"127.0.0.3",
	})
	cases := []struct {
		permF func(n int) []int
		dialF DialContext
	}{
		{
			permF: func(n int) []int {
				return []int{0}
			},
			dialF: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if got, want := addr, net.JoinHostPort("127.0.0.1", "443"); got != want {
					t.Fatalf("got addr %q, want %q", got, want)
				}
				return nil, nil
			},
		},
		{
			permF: func(n int) []int {
				return []int{1}
			},
			dialF: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if got, want := addr, net.JoinHostPort("127.0.0.2", "443"); got != want {
					t.Fatalf("got addr %q, want %q", got, want)
				}
				return nil, nil
			},
		},
		{
			permF: func(n int) []int {
				return []int{2}
			},
			dialF: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if got, want := addr, net.JoinHostPort("127.0.0.3", "443"); got != want {
					t.Fatalf("got addr %q, want %q", got, want)
				}
				return nil, nil
			},
		},
	}

	origFunc := randPerm
	defer func() {
		randPerm = origFunc
	}()

	for _, tc := range cases {
		t.Run("", func(t *testing.T) {
			randPerm = tc.permF
			if _, err := DialContextWithDNSCache(resolver, tc.dialF)(context.Background(), "tcp", "play.min.io:443"); err != nil {
				t.Fatalf("err: %s", err)
			}
		})
	}

}

func TestDialContextWithDNSCacheRand(t *testing.T) {
	rand.Seed(time.Now().UTC().UnixNano())
	defer func() {
		rand.Seed(1)
	}()

	resolver := &DNSCache{}
	resolver.cache.Store("play.min.io", []string{
		"127.0.0.1",
		"127.0.0.2",
		"127.0.0.3",
	})

	count := make(map[string]int)
	dialF := func(ctx context.Context, network, addr string) (net.Conn, error) {
		count[addr]++
		return nil, nil
	}

	for i := 0; i < 100; i++ {
		if _, err := DialContextWithDNSCache(resolver, dialF)(context.Background(), "tcp", "play.min.io:443"); err != nil {
			t.Fatalf("err: %s", err)
		}
	}

	for _, c := range count {
		got := float32(c) / float32(100)
		if got < float32(0.1) {
			t.Fatalf("expected 0.1 rate got %f", got)
		}
	}
}

// Verify without port Dial fails, Go stdlib net.Dial expects port
func TestDialContextWithDNSCacheScenario1(t *testing.T) {
	resolver := testDNSCache(t)
	if _, err := DialContextWithDNSCache(resolver, nil)(context.Background(), "tcp", "play.min.io"); err == nil {
		t.Fatalf("expect to fail") // expected port
	}
}

// Verify if the host lookup function failed to return addresses
func TestDialContextWithDNSCacheScenario2(t *testing.T) {
	if runtime.GOOS == "windows" {
		// Windows doesn't use Dial to connect
		// so there is no way this test will work
		// as expected.
		t.Skip()
	}

	res := testDNSCache(t)
	originalResolver := res.resolver
	defer func() {
		res.resolver = originalResolver
	}()

	res.resolver = &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			return nil, fmt.Errorf("err")
		},
	}

	if _, err := DialContextWithDNSCache(res, nil)(context.Background(), "tcp", "min.io:443"); err == nil {
		t.Fatalf("expect to fail")
	}
}

// Verify we always return the first storageerror from net.Dial failure
func TestDialContextWithDNSCacheScenario3(t *testing.T) {
	resolver := &DNSCache{}
	resolver.cache.Store("min.io", []string{
		"1.1.1.1",
		"2.2.2.2",
		"3.3.3.3",
	})
	origFunc := randPerm
	randPerm = func(n int) []int {
		return []int{0, 1, 2}
	}
	defer func() {
		randPerm = origFunc
	}()

	want := errors.New("error1")
	dialF := func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr == net.JoinHostPort("1.1.1.1", "443") {
			return nil, want // first storageerror should be returned
		}
		if addr == net.JoinHostPort("2.2.2.2", "443") {
			return nil, fmt.Errorf("error2")
		}
		if addr == net.JoinHostPort("3.3.3.3", "443") {
			return nil, fmt.Errorf("error3")
		}
		return nil, nil
	}

	_, got := DialContextWithDNSCache(resolver, dialF)(context.Background(), "tcp", "min.io:443")
	if got != want {
		t.Fatalf("got storageerror %v, want %v", got, want)
	}
}
