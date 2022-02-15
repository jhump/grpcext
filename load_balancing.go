package grpcext

import (
	"errors"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/grpclog"
	"google.golang.org/grpc/naming"
	"google.golang.org/grpc/status"
)

var errBalancerClosed = errors.New("grpc: balancer is closed")

// FewestActiveStreams returns a Balancer that selects addresses in a
// least-loaded fashion using this client's number of active streams as the
// measure of load. It reverts to round-robin behavior in the face of ties.
func FewestActiveStreams(r naming.Resolver) grpc.Balancer {
	return &fewerActiveStreams{r: r}
}

type addrInfo struct {
	addr      grpc.Address
	connected bool
}

// TODO: this is currently a fork of round-robin and does not actually implement least-loaded
type fewerActiveStreams struct {
	r      naming.Resolver
	w      naming.Watcher
	addrs  []*addrInfo // all the addresses the client should potentially connect
	mu     sync.Mutex
	addrCh chan []grpc.Address // the channel to notify gRPC internals the list of addresses the client should connect to.
	next   int            // index of the next address to return for Get()
	waitCh chan struct{}  // the channel to block when there is no connected address available
	done   bool           // The Balancer is closed.
}

func (rr *fewerActiveStreams) watchAddrUpdates() error {
	updates, err := rr.w.Next()
	if err != nil {
		grpclog.Warningf("grpc: the naming watcher stops working due to %v.", err)
		return err
	}
	rr.mu.Lock()
	defer rr.mu.Unlock()
	for _, update := range updates {
		addr := grpc.Address{
			Addr:     update.Addr,
			Metadata: update.Metadata,
		}
		switch update.Op {
		case naming.Add:
			var exist bool
			for _, v := range rr.addrs {
				if addr == v.addr {
					exist = true
					grpclog.Infoln("grpc: The name resolver wanted to add an existing address: ", addr)
					break
				}
			}
			if exist {
				continue
			}
			rr.addrs = append(rr.addrs, &addrInfo{addr: addr})
		case naming.Delete:
			for i, v := range rr.addrs {
				if addr == v.addr {
					copy(rr.addrs[i:], rr.addrs[i+1:])
					rr.addrs = rr.addrs[:len(rr.addrs)-1]
					break
				}
			}
		default:
			grpclog.Errorln("Unknown update.Op ", update.Op)
		}
	}
	// Make a copy of rr.addrs and write it onto rr.addrCh so that gRPC internals gets notified.
	open := make([]grpc.Address, len(rr.addrs))
	for i, v := range rr.addrs {
		open[i] = v.addr
	}
	if rr.done {
		return grpc.ErrClientConnClosing
	}
	select {
	case <-rr.addrCh:
	default:
	}
	rr.addrCh <- open
	return nil
}

func (rr *fewerActiveStreams) Start(target string, config grpc.BalancerConfig) error {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	if rr.done {
		return grpc.ErrClientConnClosing
	}
	if rr.r == nil {
		// If there is no name resolver installed, it is not needed to
		// do name resolution. In this case, target is added into rr.addrs
		// as the only address available and rr.addrCh stays nil.
		rr.addrs = append(rr.addrs, &addrInfo{addr: grpc.Address{Addr: target}})
		return nil
	}
	w, err := rr.r.Resolve(target)
	if err != nil {
		return err
	}
	rr.w = w
	rr.addrCh = make(chan []grpc.Address, 1)
	go func() {
		for {
			if err := rr.watchAddrUpdates(); err != nil {
				return
			}
		}
	}()
	return nil
}

// Up sets the connected state of addr and sends notification if there are pending
// Get() calls.
func (rr *fewerActiveStreams) Up(addr grpc.Address) func(error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	var cnt int
	for _, a := range rr.addrs {
		if a.addr == addr {
			if a.connected {
				return nil
			}
			a.connected = true
		}
		if a.connected {
			cnt++
		}
	}
	// addr is only one which is connected. Notify the Get() callers who are blocking.
	if cnt == 1 && rr.waitCh != nil {
		close(rr.waitCh)
		rr.waitCh = nil
	}
	return func(err error) {
		rr.down(addr, err)
	}
}

// down unsets the connected state of addr.
func (rr *fewerActiveStreams) down(addr grpc.Address, err error) {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	for _, a := range rr.addrs {
		if addr == a.addr {
			a.connected = false
			break
		}
	}
}

// Get returns the next addr in the rotation.
func (rr *fewerActiveStreams) Get(ctx context.Context, opts grpc.BalancerGetOptions) (addr grpc.Address, put func(), err error) {
	var ch chan struct{}
	rr.mu.Lock()
	if rr.done {
		rr.mu.Unlock()
		err = grpc.ErrClientConnClosing
		return
	}

	if len(rr.addrs) > 0 {
		if rr.next >= len(rr.addrs) {
			rr.next = 0
		}
		next := rr.next
		for {
			a := rr.addrs[next]
			next = (next + 1) % len(rr.addrs)
			if a.connected {
				addr = a.addr
				rr.next = next
				rr.mu.Unlock()
				return
			}
			if next == rr.next {
				// Has iterated all the possible address but none is connected.
				break
			}
		}
	}
	if !opts.BlockingWait {
		if len(rr.addrs) == 0 {
			rr.mu.Unlock()
			err = status.Errorf(codes.Unavailable, "there is no address available")
			return
		}
		// Returns the next addr on rr.addrs for failfast RPCs.
		addr = rr.addrs[rr.next].addr
		rr.next++
		rr.mu.Unlock()
		return
	}
	// Wait on rr.waitCh for non-failfast RPCs.
	if rr.waitCh == nil {
		ch = make(chan struct{})
		rr.waitCh = ch
	} else {
		ch = rr.waitCh
	}
	rr.mu.Unlock()
	for {
		select {
		case <-ctx.Done():
			err = ctx.Err()
			return
		case <-ch:
			rr.mu.Lock()
			if rr.done {
				rr.mu.Unlock()
				err = grpc.ErrClientConnClosing
				return
			}

			if len(rr.addrs) > 0 {
				if rr.next >= len(rr.addrs) {
					rr.next = 0
				}
				next := rr.next
				for {
					a := rr.addrs[next]
					next = (next + 1) % len(rr.addrs)
					if a.connected {
						addr = a.addr
						rr.next = next
						rr.mu.Unlock()
						return
					}
					if next == rr.next {
						// Has iterated all the possible address but none is connected.
						break
					}
				}
			}
		// The newly added addr got removed by Down() again.
			if rr.waitCh == nil {
				ch = make(chan struct{})
				rr.waitCh = ch
			} else {
				ch = rr.waitCh
			}
			rr.mu.Unlock()
		}
	}
}

func (rr *fewerActiveStreams) Notify() <-chan []grpc.Address {
	return rr.addrCh
}

func (rr *fewerActiveStreams) Close() error {
	rr.mu.Lock()
	defer rr.mu.Unlock()
	if rr.done {
		return errBalancerClosed
	}
	rr.done = true
	if rr.w != nil {
		rr.w.Close()
	}
	if rr.waitCh != nil {
		close(rr.waitCh)
		rr.waitCh = nil
	}
	if rr.addrCh != nil {
		close(rr.addrCh)
	}
	return nil
}

// ConnectionLimits defines configuration for limiting the number of connections
// that a client maintains. This is particularly useful when communicating with
// a for which a naming service returns a very large list of address -- more
// addresses than the client should realistically use.
type ConnectionLimits struct {
	// The initial number of connections that the client will try to open. If
	// a name service reports fewer addresses, it will use that number instead
	// (e.g. one connection per address). This is the number of connections
	// maintained by the client, even when it is idle.
	InitialConns        int

	// The maximum number of connections that the client will maintain. This is
	// a hard limit.
	MaxConns        int

	// The desired number of active streams per connection. As the client
	// sends more and more traffic, this number is used to scale the actual
	// number of connections up or down. As the actual number of streams per
	// connection rises, this will cause connections to be created up to a one
	// per address reported by the name service or MaxTotalConns, whichever is
	// lower.
	ActiveStreamsPerAddress int

	// The maximum number of streams the client should use with a single
	// connection. This can be used to force the client to establish redundant
	// connections -- more than one connection to the same address. This may be
	// useful in the event that a single connection is insufficient to saturate
	// network capacity: an extra connection may be able to increase throughput.
	MaxStreamsPerConn int

	// The minimum amount a time the client will try to keep a connection open.
	// This smooths out volatility in the number of connections that might
	// otherwise arise from volatility in the amount of traffic the client is
	// sending. Instead of immediately closing and re-opening connections, to
	// maintain the desired ActiveStreamsPerConn, this will keep connections
	// around for a minimum amount of time.
	MinConnectTime time.Duration

	// How often connections are re-balanced. The re-balance operation is a
	// no-op if the client has one connection to every address returned by the
	// name service. If it has more than one connection to any address, that
	// connection will be moved to another host (so the extra load is shifted
	// around). If it has fewer than one connection to every address, it will
	// close one connection and choose a new address (so the load is better
	// dispersed throughout the addresses returned by the name service, despite
	// the limited number of connections maintained).
	RebalancePeriod time.Duration
}

// WithConnectionLimits wraps the given balancer, enforcing limits on the number
// of addresses actually sent to the GRPC client. The given limits parameter
// controls the number of actual addresses that the balancer will use. In order
// to achieve multiple connections to a single address (to enforce a minimum
// number of connections or maximum number of streams per connection), this
// balancer will wrap incoming address metadata with one that also includes a
// counter.
func WithConnectionLimits(b grpc.Balancer, limits ConnectionLimits) grpc.Balancer {
	// TODO: implement me!
	return nil
}