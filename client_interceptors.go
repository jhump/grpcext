package grpcext

import (
	"fmt"
	"io"
	"reflect"
	"sync"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
)

// WithUnaryClientInterceptors returns a client DialOption, for configuring a GRPC client that uses the
// given interceptors with unary RPC methods.
func WithUnaryClientInterceptors(interceptors ...grpc.UnaryClientInterceptor) grpc.DialOption {
	return grpc.WithUnaryInterceptor(CombineUnaryClientInterceptors(interceptors...))
}

// WithClientInterceptorsAsUnary returns a client DialOption, for configuring a GRPC client that uses the
// given interceptors with unary RPC methods. The given interceptors can be either unary or stream
// interceptors. Stream interceptors will be adapted to unary RPCs via StreamClientInterceptorToUnary.
func WithClientInterceptorsAsUnary(interceptors ...interface{}) grpc.DialOption {
	return grpc.WithUnaryInterceptor(combineClientInterceptorsIntoUnary(interceptors))
}

func combineClientInterceptorsIntoUnary(interceptors []interface{}) grpc.UnaryClientInterceptor {
	var unary []grpc.UnaryClientInterceptor
	var streams []grpc.StreamClientInterceptor
	for _, i := range interceptors {
		switch i := i.(type) {
		case grpc.UnaryClientInterceptor:
			if len(streams) > 0 {
				u := StreamClientInterceptorToUnary(CombineStreamClientInterceptors(streams...))
				unary = append(unary, u)
				streams = nil
			}
			unary = append(unary, i)
		case grpc.StreamClientInterceptor:
			streams = append(streams, i)
		default:
			panic(fmt.Sprintf("Given argument is neither a unary nor stream client interceptor: %v", reflect.TypeOf(i)))
		}
	}
	if len(streams) > 0 {
		u := StreamClientInterceptorToUnary(CombineStreamClientInterceptors(streams...))
		unary = append(unary, u)
	}
	return CombineUnaryClientInterceptors(unary...)
}

// WithStreamClientInterceptors returns a client DialOption, for configuring a GRPC client that uses the
// given interceptors with streaming RPC methods.
func WithStreamClientInterceptors(interceptors ...grpc.StreamClientInterceptor) grpc.DialOption {
	return grpc.WithStreamInterceptor(CombineStreamClientInterceptors(interceptors...))
}

// CombineUnaryClientInterceptors combines the given interceptors, in order, and returns a single
// resulting interceptor. The first interceptor given will be the first one invoked. It is passed
// an invoker that will actually call the next interceptor in the slice, and so on. The final
// interceptor given will be passed the original invoker.
func CombineUnaryClientInterceptors(interceptors ...grpc.UnaryClientInterceptor) grpc.UnaryClientInterceptor {
	if len(interceptors) == 0 {
		return nil
	}
	if len(interceptors) == 1 {
		return interceptors[0]
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
	if len(interceptors) == 1 {
		return interceptors[0]
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
func StreamClientInterceptorToUnary(interceptor grpc.StreamClientInterceptor) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		streamer, err := invokerAsStreamer(reflect.TypeOf(reply), invoker)
		if err != nil {
			return err
		}
		ctx, cancel := context.WithCancel(ctx)
		defer cancel()
		cs, err := interceptor(ctx, &grpc.StreamDesc{StreamName: method}, cc, method, streamer, opts...)
		if err != nil {
			return err
		}
		err = cs.SendMsg(req)
		if err != nil {
			return err
		}
		err = cs.CloseSend()
		if err != nil {
			return err
		}
		err = cs.RecvMsg(reply)
		if err != nil {
			return err
		}
		err = cs.RecvMsg(reply)
		if err == nil {
			return grpc.Errorf(codes.Internal, "Too many response messages sent")
		} else if err != io.EOF {
			return err
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
	stateSendingClosed = 1 << iota
	stateAwaitingHeaders
	stateAwaitingResponse
)

func (c *unaryClientStream) Header() (metadata.MD, error) {
	c.changeState(stateAwaitingHeaders, nil)
	<-c.ctx.Done()
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
	<-c.ctx.Done()
	if c.err != nil {
		return c.err
	}

	// make sure response is populated correctly
	c.mu.Lock()
	defer c.mu.Unlock()

	// if resp is nil *after* RPC is done, then we've sent back a response
	// and this is a request for second (or third, etc...)
	if c.resp == nil {
		return io.EOF
	}

	if c.resp != m {
		// c.resp is not nil but is not expected instance? copy into m
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

func (c *unaryClientStream) changeState(s int, m interface{}) {
	c.mu.Lock()
	defer c.mu.Unlock()

	prevState := c.state
	c.state |= s
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

func readyToInvoke(state int) bool {
	// we are ready to invoke RPC if we've closed sending and are awaiting something
	// (either headers or response)
	return (state&stateSendingClosed) != 0 &&
		(state&(stateAwaitingHeaders|stateAwaitingResponse)) != 0
}
