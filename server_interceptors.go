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

// WithServerInterceptorsUnary returns a ServerOption, for configuring a GRPC server that uses the
// given interceptors with unary RPC methods.
func WithServerInterceptorsUnary(interceptors ...ServerInterceptor) grpc.ServerOption {
	return grpc.UnaryInterceptor(ServerInterceptorAsGrpcUnary(CombineServerInterceptors(interceptors)))
}

// WithServerInterceptorsStream returns a ServerOption, for configuring a GRPC server that uses the
// given interceptors with streaming RPC methods.
func WithServerInterceptorsStream(interceptors ...ServerInterceptor) grpc.ServerOption {
	return grpc.StreamInterceptor(ServerInterceptorAsGrpcStream(CombineServerInterceptors(interceptors)))
}

// CombineUnaryServerInterceptors combines the given interceptors, in order, and returns a single
// resulting interceptor. The first interceptor given will be the first one invoked. It is passed
// a handler that will actually invoke the next interceptor in the slice, and so on. The final
// interceptor given will be passed the original handler.
func CombineUnaryServerInterceptors(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	if len(interceptors) == 0 {
		return nil
	}
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		return serverProceedUnary(ctx, req, info, handler, interceptors)
	}
}

func serverProceedUnary(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler, ints []grpc.UnaryServerInterceptor) (interface{}, error) {
	if len(ints) == 0 {
		return handler(ctx, req)
	} else {
		interceptor := ints[0]
		return interceptor(ctx, req, info, func(ctx context.Context, req interface{}) (interface{}, error) {
			return serverProceedUnary(ctx, req, info, handler, ints[1:])
		})
	}
}

// CombineStreamServerInterceptors combines the given interceptors, in order, and returns a single
// resulting interceptor. The first interceptor given will be the first one invoked. It is passed
// a handler that will actually invoke the next interceptor in the slice, and so on. The final
// interceptor given will be passed the original handler.
func CombineStreamServerInterceptors(interceptors ...grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	if len(interceptors) == 0 {
		return nil
	}
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return serverProceedStream(srv, ss, info, handler, interceptors)
	}
}

func serverProceedStream(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler, ints []grpc.StreamServerInterceptor) error {
	if len(ints) == 0 {
		return handler(srv, ss)
	} else {
		interceptor := ints[0]
		return interceptor(srv, ss, info, func(srv interface{}, stream grpc.ServerStream) error {
			return serverProceedStream(srv, stream, info, handler, ints[1:])
		})
	}
}

// StreamServerInterceptorToUnary converts a GRPC stream interceptor into an equivalent unary
// interceptor. This lets you re-use stream interceptors for all kinds of RPCs.
//
// It is recommended to instead implement the ServerInterceptor interface (in this package),
// instead of either of the GRPC interceptor interfaces. While slightly less powerful, they are
// simpler to implement and are more easily adapted to both kinds of GRPC calls (so the result
// will be a more efficient unary interceptor than what this function returns).
func StreamServerInterceptorToUnary(interceptor grpc.StreamServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		stream := &unaryServerStream{ctx: ctx, req: req}
		streamHandler, err := handlerAsStream(reflect.TypeOf(req), handler)
		if err != nil { return nil, err }
		err = interceptor(info.Server, stream, &grpc.StreamServerInfo{FullMethod: info.FullMethod}, streamHandler)
		if err != nil { return nil, err }
		if stream.resp == nil {
			return nil, grpc.Errorf(codes.Internal, "Missing response message")
		}
		return stream.resp, nil
	}
}

func handlerAsStream(reqType reflect.Type, handler grpc.UnaryHandler) (grpc.StreamHandler, error) {
	if reqType.Kind() != reflect.Ptr {
		return nil, grpc.Errorf(codes.Internal, "Stream adapter encountered unsupported non-pointer request type: %v", reqType)
	}
	return func(srv interface{}, stream grpc.ServerStream) error {
		v := reflect.New(reqType.Elem()).Interface()
		if err := stream.RecvMsg(v); err != nil {
			return err
		}
		// verify that stream half-closed
		if err := stream.RecvMsg(v); err != io.EOF {
			if err == nil {
				return grpc.Errorf(codes.InvalidArgument, "Too many request messages sent")
			}
			return err
		}

		// invoke handler and send back the response
		resp, err := handler(stream.Context(), v)
		if err != nil {
			return err
		}
		return stream.SendMsg(resp)
	}, nil
}

// unaryServerStream implements the ServerStream interface for unary RPC operations
type unaryServerStream struct {
	ctx  context.Context
	mu   sync.Mutex
	req  interface{}
	resp interface{}
}

func (s *unaryServerStream) SetHeader(md metadata.MD) error {
	return grpc.SetHeader(s.ctx, md)
}

func (s *unaryServerStream) SendHeader(md metadata.MD) error {
	return grpc.SendHeader(s.ctx, md)
}

func (s *unaryServerStream) SetTrailer(md metadata.MD) {
	if err := grpc.SetTrailer(s.ctx, md); err != nil {
		panic(err)
	}
}

func (s *unaryServerStream) Context() context.Context {
	return s.ctx
}

func (s *unaryServerStream) SendMsg(m interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.resp != nil {
		return grpc.Errorf(codes.Internal, "Stream tried to send multiple response messages for unary RPC")
	}
	s.resp = m
	return nil
}

func (s *unaryServerStream) RecvMsg(m interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.req == nil {
		return io.EOF
	}

	reqValue := reflect.ValueOf(m)
	sourceValue := reflect.ValueOf(s.req)
	reqType := reqValue.Type()
	expectedType := sourceValue.Type()
	if reqType != expectedType {
		return grpc.Errorf(codes.Internal, "Stream adapter requested to populate unsupported request type: got %v, expecting %v", reqType, expectedType)
	} else if reqType.Kind() != reflect.Ptr {
		return grpc.Errorf(codes.Internal, "Stream adapter encountered unsupported non-pointer request type: %v", reqType)
	}

	// copy source into given request object
	reqValue.Elem().Set(sourceValue.Elem())

	s.req = nil
	return nil
}

// ServerInterceptor provides a limited interface for intercepting calls and exchanged messages
// that can easily be implemented to handle both unary and streaming RPCs. Similarly, it can easily
// be adapted to GRPC's unary and streaming interceptor interfaces.
//
// The interceptor must call the given Invocation in order to continue servicing the stream.
type ServerInterceptor func(ctx context.Context, srv interface{}, info *grpc.StreamServerInfo, proceed ServerInvocation) error

// ServerInterceptorAsGrpcUnary converts an interceptor into an equivalent GRPC unary interceptor.
// If multiple interceptors, after being converted to GRPC unary interceptors, are combined then
// they are invoked in the order they are combined: e.g. first one in the combined list is the
// first one invoked.
//
// Similarly, if the interceptors provide message observers as they proceed, the received message
// (e.g. the request) is first observed by the first interceptor's observer.
//
// However, message observers for the sent message (e.g. the response) are invoked "inside out".
// So when the handler returns the response message, the last interceptor's observer is the first
// one invoked. As the interceptor stack unwinds, message observers are invoked working their way
// back to the first one.
func ServerInterceptorAsGrpcUnary(i ServerInterceptor) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		var resp interface{}
		proceed := func(ctx context.Context, reqObs MessageObserver, respObs MessageObserver) error {
			if reqObs != nil {
				if err := reqObs(req); err != nil {
					return err
				}
			}
			if r, err := handler(ctx, req); err != nil {
				return err
			} else {
				if respObs != nil {
					if err := respObs(r); err != nil {
						return err
					}
				}
				resp = r
				return nil
			}
		}
		err := i(ctx, info.Server, &grpc.StreamServerInfo{FullMethod: info.FullMethod}, proceed)
		return resp, err
	}
}

// ServerInterceptorAsGrpcStream converts an interceptor into an equivalent GRPC stream
// interceptor. If multiple interceptors, after being converted to GRPC stream interceptors, are
// combined then they are invoked in the order they are combined: e.g. first one in the combined
// list is the first one invoked.
//
// Similarly, if the interceptors provide message observers as they proceed, the received messages
// (e.g. requests) are first observed by the first interceptor's observer, making their way down to
// the last one and ultimately returned to the handler for processing.
//
// However, message observers for sent messages (e.g. the responses) are invoked "inside out". When
// the handler sends a message, the last interceptor's observer is the first one invoked. The
// messages makes it way back to the first interceptor and ultimately sent on the wire.
func ServerInterceptorAsGrpcStream(i ServerInterceptor) grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		proceed := func(ctx context.Context, reqObs MessageObserver, respObs MessageObserver) error {
			if reqObs != nil || respObs != nil {
				ss = observingServerStream{ctx, reqObs, respObs, ss}
			} else if ss.Context() != ctx {
				ss = contextServerStream{ctx, ss}
			}
			return handler(srv, ss)
		}
		return i(ss.Context(), srv, info, proceed)
	}
}

// observingServerStream routes all messages sent and received through given message observers and
// can also override the context. All other operations are passed through to the underlying stream.
type observingServerStream struct {
	ctx     context.Context
	reqObs  MessageObserver
	respObs MessageObserver
	ss      grpc.ServerStream
}

func (s observingServerStream) SetHeader(md metadata.MD) error {
	return s.ss.SetHeader(md)
}

func (s observingServerStream) SendHeader(md metadata.MD) error {
	return s.ss.SendHeader(md)
}

func (s observingServerStream) SetTrailer(md metadata.MD) {
	s.ss.SetTrailer(md)
}

func (s observingServerStream) Context() context.Context {
	return s.ctx
}

func (s observingServerStream) SendMsg(m interface{}) error {
	if s.respObs != nil {
		if err := s.respObs(m); err != nil {
			return err
		}
	}
	return s.ss.SendMsg(m)
}

func (s observingServerStream) RecvMsg(m interface{}) error {
	if err := s.ss.RecvMsg(m); err != nil {
		return err
	}
	if s.reqObs != nil {
		return s.reqObs(m)
	}
	return nil
}

// contextServerStream overrides the context of another stream. All methods other than Context()
// are passed through to the underlying stream.
type contextServerStream struct {
	ctx context.Context
	ss  grpc.ServerStream
}

func (s contextServerStream) SetHeader(md metadata.MD) error {
	return s.ss.SetHeader(md)
}

func (s contextServerStream) SendHeader(md metadata.MD) error {
	return s.ss.SendHeader(md)
}

func (s contextServerStream) SetTrailer(md metadata.MD) {
	s.ss.SetTrailer(md)
}

func (s contextServerStream) Context() context.Context {
	return s.ctx
}

func (s contextServerStream) SendMsg(m interface{}) error {
	return s.ss.SendMsg(m)
}

func (s contextServerStream) RecvMsg(m interface{}) error {
	return s.ss.RecvMsg(m)
}

// CombineServerInterceptors combines the given slice of interceptors into a single interceptor.
// The first interceptor in the slice is the first invoked and so on.
func CombineServerInterceptors(interceptors []ServerInterceptor) ServerInterceptor {
	if len(interceptors) == 0 {
		return nil
	}
	return func(ctx context.Context, srv interface{}, info *grpc.StreamServerInfo, proceed ServerInvocation) error {
		return serverProceed(ctx, srv, info, proceed, interceptors, nil, nil)
	}
}

func serverProceed(ctx context.Context, srv interface{}, info *grpc.StreamServerInfo, handler ServerInvocation, ints []ServerInterceptor, reqObs []MessageObserver, respObs []MessageObserver) error {
	if len(ints) == 0 {
		return handler(ctx, mergeRequestObservers(reqObs), mergeResponseObservers(respObs))
	} else {
		interceptor := ints[0]
		return interceptor(ctx, srv, info, func(ctx context.Context, reqOb MessageObserver, respOb MessageObserver) error {
			reqs := reqObs
			if reqOb != nil {
				reqs = append(reqs, reqOb)
			}
			resps := respObs
			if respOb != nil {
				resps = append(resps, respOb)
			}
			return serverProceed(ctx, srv, info, handler, ints[1:], reqs, resps)
		})
	}
}