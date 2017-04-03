package grpcext

import (
	"io"
	"reflect"
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// WithClientInterceptorsUnary returns a client DialOption, for configuring a GRPC client that uses the
// given interceptors with unary RPC methods.
func WithClientInterceptorsUnary(interceptors ...ClientInterceptor) grpc.DialOption {
	return grpc.WithUnaryInterceptor(ClientInterceptorAsGrpcUnary(combineClientInterceptors(interceptors)))
}

// WithClientInterceptorsStream returns a client DialOption, for configuring a GRPC client that uses the
// given interceptors with streaming RPC methods.
func WithClientInterceptorsStream(interceptors ...ClientInterceptor) grpc.DialOption {
	return grpc.WithStreamInterceptor(ClientInterceptorAsGrpcStream(combineClientInterceptors(interceptors)))
}

// CombineUnaryClientInterceptors combines the given interceptors, in order, and returns a single
// resulting interceptor. The first interceptor given will be the first one invoked. It is passed
// an invoker that will actually call the next interceptor in the slice, and so on. The final
// interceptor given will be passed the original invoker.
func CombineUnaryClientInterceptors(interceptors ...grpc.UnaryClientInterceptor) grpc.UnaryClientInterceptor {
	if len(interceptors) == 0 {
		return nil
	}
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return clientProceedUnary(ctx, method, req, reply, cc, invoker, opts, interceptors)
	}
}

func clientProceedUnary(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts []grpc.CallOption, ints []grpc.UnaryClientInterceptor) error {
	if len(ints) == 0 {
		return invoker(ctx, method, req, reply, cc, opts...)
	} else {
		interceptor := ints[0]
		return interceptor(ctx, method, req, reply, cc, func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			return clientProceedUnary(ctx, method, req, reply, cc, invoker, opts, ints[1:])
		}, opts...)
	}
}

// CombineStreamClientInterceptors combines the given interceptors, in order, and returns a single
// resulting interceptor. The first interceptor given will be the first one invoked. It is passed
// a streamer that will actually invoke the next interceptor in the slice, and so on. The final
// interceptor given will be passed the original streamer.
func CombineStreamClientInterceptors(interceptors ...grpc.StreamClientInterceptor) grpc.StreamClientInterceptor {
	if len(interceptors) == 0 {
		return nil
	}
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return clientProceedStream(ctx, desc, cc, method, streamer, opts, interceptors)
	}
}

func clientProceedStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts []grpc.CallOption, ints []grpc.StreamClientInterceptor) (grpc.ClientStream, error) {
	if len(ints) == 0 {
		return streamer(ctx, desc, cc, method, opts...)
	} else {
		interceptor := ints[0]
		return interceptor(ctx, desc, cc, method, func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return clientProceedStream(ctx, desc, cc, method, streamer, opts, ints[1:])
		}, opts...)
	}
}

// StreamClientInterceptorToUnary converts a GRPC stream interceptor into an equivalent unary
// interceptor. This lets you re-use stream interceptors for all kinds of RPCs.
//
// It is recommended to instead implement the ClientInterface interface (in this package),
// instead of either of the GRPC interceptor interfaces. While slightly less powerful, they are
// simpler to implement and are more easily adapted to both kinds of GRPC calls (so the result
// will be a more efficient unary interceptor than what this function returns).
func StreamClientInterceptorToUnary(interceptor grpc.StreamClientInterceptor) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		streamer, err := invokerAsStreamer(reflect.TypeOf(reply), invoker)
		if err != nil { return err }
		cs, err := interceptor(ctx, &grpc.StreamDesc{StreamName: method}, cc, method, streamer, opts...)
		if err != nil { return err }
		err = cs.SendMsg(req)
		if err != nil { return err }
		err = cs.CloseSend()
		if err != nil { return err }
		err = cs.RecvMsg(reply)
		if err != nil { return err }
		err = cs.RecvMsg(reply)
		if err != io.EOF {
			return grpc.Errorf(codes.Internal, "Too many response messages sent")
		}
		return nil
	}
}

func invokerAsStreamer(respType reflect.Type, invoker grpc.UnaryInvoker) (grpc.Streamer, error) {
	if respType.Kind() != reflect.Ptr {
		return nil, grpc.Errorf(codes.InvalidArgument, "Stream adapter encountered unsupported non-pointer response type: %v", respType)
	}
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		ctx, cancel := context.WithCancel(ctx)
		return &unaryClientStream{ctx: ctx, finish: cancel, respType: respType, cc: cc, method: method, opts: opts, invoker: invoker}, nil
	}, nil
}

// unaryClientStream implements the ClientStream interface for unary RPC operations
type unaryClientStream struct {
	ctx      context.Context
	finish   context.CancelFunc
	respType reflect.Type
	cc       *grpc.ClientConn
	method   string
	opts     []grpc.CallOption
	invoker  grpc.UnaryInvoker

	// These values are only written while holding mutex below. But they are also
	// safely published via ctx's done channel. So they can be read without mutex
	// if reader has observed that context is done.
	header  metadata.MD
	trailer metadata.MD
	err     error

	mu    sync.Mutex
	req   interface{}
	resp  interface{}
	state int
}

const (
	stateAwaitingHeaders = 1 << iota
	stateSendingClosed
	stateAwaitingResponse
)

func (c *unaryClientStream) Header() (metadata.MD, error) {
	c.changeState(stateAwaitingHeaders, nil)
	<- c.ctx.Done()
	return c.header, c.err
}

func (c *unaryClientStream) Trailer() metadata.MD {
	if c.ctx.Err() == nil {
		// not done yet
		panic("RPC not done!")
	}
	return c.trailer
}

func (c *unaryClientStream) Context() context.Context {
	return c.ctx
}

func (c *unaryClientStream) CloseSend() error {
	if c.req == nil {
		return grpc.Errorf(codes.InvalidArgument, "Missing request message")
	}
	c.changeState(stateSendingClosed, nil)
	if c.ctx.Err() != nil {
		return c.err
	}
	return nil
}

func (c *unaryClientStream) SendMsg(m interface{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.req != nil {
		return grpc.Errorf(codes.InvalidArgument, "Stream tried to send multiple request messages for unary RPC")
	}
	c.req = m
	return nil
}

func (c *unaryClientStream) RecvMsg(m interface{}) error {
	c.changeState(stateAwaitingResponse, m)
	<- c.ctx.Done()
	if c.err != nil { return c.err }

	// make sure response is populated correctly
	c.mu.Lock()
	defer c.mu.Unlock()

	// if resp is nil *after* RPC is done, then we've sent back a response
	// and this is a request for second (or third, etc...)
	if c.resp == nil {
		return io.EOF
	}

	if c.resp != m {
		// c.resp is not nil but is wrong instance? copy into m
		respValue := reflect.ValueOf(m)
		sourceValue := reflect.ValueOf(c.resp)
		respType := respValue.Type()
		expectedType := sourceValue.Type()
		if respType != expectedType {
			return grpc.Errorf(codes.InvalidArgument, "Stream adapter requested to populate unsupported response type: got %v, expecting %v", respType, expectedType)
		} else if respType.Kind() != reflect.Ptr {
			return grpc.Errorf(codes.InvalidArgument, "Stream adapter encountered unsupported non-pointer response type: %v", respType)
		}
		reflect.ValueOf(m).Elem().Set(reflect.ValueOf(c.resp).Elem())
	}
	// clear c.resp before leaving
	c.resp = nil
	return nil
}

func (c *unaryClientStream) changeState(f int, m interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	prevState := c.addStateLocked(f)
	if readyToInvoke(c.state) && !readyToInvoke(prevState) {
		// we've transitioned to a state where we can invoke!
		if m == nil {
			m = reflect.New(c.respType.Elem()).Interface()
		}
		c.resp = m
		c.err = c.invoker(c.ctx, c.method, c.req, m, c.cc, c.opts...)
		c.finish()
	}
}

func (c *unaryClientStream) addStateLocked(s int) int {
	prev := c.state
	c.state |= s
	return prev
}

func readyToInvoke(state int) bool {
	// we are ready to invoke RPC if we've closed sending and are awaiting something
	// (either headers or response)
	return (state & stateSendingClosed) != 0 &&
			(state & (stateAwaitingHeaders | stateAwaitingResponse)) != 0
}

// ClientInterceptor provides a limited interface for intercepting calls and exchanged messages
// that can easily be implemented to handle both unary and streaming RPCs. Similarly, it can easily
// be adapted to GRPC's unary and streaming interceptor interfaces.
//
// The interceptor must call the given Invocation in order to continue servicing the stream.
type ClientInterceptor func(ctx context.Context, cc *grpc.ClientConn, info *grpc.StreamServerInfo, proceed Invocation) error

// ClientInterceptorAsGrpcUnary converts an interceptor into an equivalent GRPC unary interceptor.
// If multiple interceptors, after being converted to GRPC unary interceptors, are combined then
// they are invoked in the order they are combined: e.g. first one in the combined list is the
// first one invoked.
//
// Similarly, if the interceptors provide message observers as they proceed, the sent message
// (e.g. the request) is first observed by the first interceptor's observer.
//
// However, message observers for the received message (e.g. the response) are invoked "inside out".
// So when the invoker returns the response message, the last interceptor's observer is the first
// one invoked. As the interceptor stack unwinds, message observers are invoked working their way
// back to the first one.
func ClientInterceptorAsGrpcUnary(i ClientInterceptor) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		proceed := func(ctx context.Context, reqObs MessageObserver, respObs MessageObserver) error {
			if reqObs != nil {
				if err := reqObs(req); err != nil {
					return err
				}
			}
			if err := invoker(ctx, method, req, reply, cc, opts...); err != nil {
				return err
			} else {
				if respObs != nil {
					if err := respObs(reply); err != nil {
						return err
					}
				}
				return nil
			}
		}
		return i(ctx, cc, &grpc.StreamServerInfo{FullMethod: method}, proceed)
	}
}

// ClientInterceptorAsGrpcStream converts an interceptor into an equivalent GRPC stream
// interceptor. If multiple interceptors, after being converted to GRPC stream interceptors, are
// combined then they are invoked in the order they are combined: e.g. first one in the combined
// list is the first one invoked.
//
// Similarly, if the interceptors provide message observers as they proceed, the sent messages
// (e.g. requests) are first observed by the first interceptor's observer, making their way down to
// the last one and ultimately sent to the server.
//
// However, message observers for received messages (e.g. the responses) are invoked "inside out".
// When the client receives a message, the last interceptor's observer is the first one invoked. The
// messages makes it way back to the first interceptor and then to the caller.
func ClientInterceptorAsGrpcStream(i ClientInterceptor) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		var cs grpc.ClientStream
		proceed := func(ctx context.Context, reqObs MessageObserver, respObs MessageObserver) error {
			stream, err := streamer(ctx, desc, cc, method, opts...)
			if err != nil {
				return err
			}
			if reqObs != nil || respObs != nil {
				cs = observingClientStream{ctx, reqObs, respObs, stream}
			} else if stream.Context() != ctx {
				cs = contextClientStream{ctx, stream}
			} else {
				cs = stream
			}
			return nil
		}
		info := &grpc.StreamServerInfo{
			FullMethod: method,
			IsClientStream: desc.ClientStreams,
			IsServerStream: desc.ServerStreams,
		}
		return cs, i(ctx, cc, info, proceed)
	}
}

// observingClientStream routes all messages sent and received through given message observers and
// can also override the context. All other operations are passed through to the underlying stream.
type observingClientStream struct {
	ctx     context.Context
	reqObs  MessageObserver
	respObs MessageObserver
	cs      grpc.ClientStream
}

func (c observingClientStream) Header() (metadata.MD, error) {
	return c.cs.Header()
}

func (c observingClientStream) Trailer() metadata.MD {
	return c.cs.Trailer()
}

func (c observingClientStream) Context() context.Context {
	return c.ctx
}

func (c observingClientStream) CloseSend() error {
	return c.cs.CloseSend()
}

func (c observingClientStream) SendMsg(m interface{}) error {
	if c.reqObs != nil {
		if err := c.reqObs(m); err != nil {
			return err
		}
	}
	return c.cs.SendMsg(m)
}

func (c observingClientStream) RecvMsg(m interface{}) error {
	if err := c.cs.RecvMsg(m); err != nil {
		return err
	}
	if c.respObs != nil {
		return c.respObs(m)
	}
	return nil
}

// contextClientStream overrides the context of another stream. All methods other than Context()
// are passed through to the underlying stream.
type contextClientStream struct {
	ctx context.Context
	cs  grpc.ClientStream
}

func (c contextClientStream) Header() (metadata.MD, error) {
	return c.cs.Header()
}

func (c contextClientStream) Trailer() metadata.MD {
	return c.cs.Trailer()
}

func (c contextClientStream) Context() context.Context {
	return c.ctx
}

func (c contextClientStream) CloseSend() error {
	return c.cs.CloseSend()
}


func (c contextClientStream) SendMsg(m interface{}) error {
	return c.cs.SendMsg(m)
}

func (c contextClientStream) RecvMsg(m interface{}) error {
	return c.cs.RecvMsg(m)
}

// combineClientInterceptors combines the given slice of interceptors into a single interceptor.
// The first interceptor in the slice is the first invoked and so on.
func combineClientInterceptors(interceptors []ClientInterceptor) ClientInterceptor {
	if len(interceptors) == 0 {
		return nil
	}
	return func(ctx context.Context, cc *grpc.ClientConn, info *grpc.StreamServerInfo, proceed Invocation) error {
		return clientProceed(ctx, cc, info, proceed, interceptors, nil, nil)
	}
}

func clientProceed(ctx context.Context, cc *grpc.ClientConn, info *grpc.StreamServerInfo, handler Invocation, ints []ClientInterceptor, reqObs []MessageObserver, respObs []MessageObserver) error {
	if len(ints) == 0 {
		return handler(ctx, mergeRequestObservers(reqObs), mergeResponseObservers(respObs))
	} else {
		interceptor := ints[0]
		return interceptor(ctx, cc, info, func(ctx context.Context, reqOb MessageObserver, respOb MessageObserver) error {
			reqs := reqObs
			if reqOb != nil {
				reqs = append(reqs, reqOb)
			}
			resps := respObs
			if respOb != nil {
				resps = append(resps, respOb)
			}
			return clientProceed(ctx, cc, info, handler, ints[1:], reqs, resps)
		})
	}
}