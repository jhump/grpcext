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

// WithUnaryServerInterceptors returns a ServerOption, for configuring a GRPC server that uses the
// given interceptors with unary RPC methods.
func WithUnaryServerInterceptors(interceptors ...grpc.UnaryServerInterceptor) grpc.ServerOption {
	return grpc.UnaryInterceptor(CombineUnaryServerInterceptors(interceptors...))
}

// WithServerInterceptorsAsUnary returns a ServerOption, for configuring a GRPC server that uses the
// given interceptors with unary RPC methods. The given interceptors can be either unary or stream
// interceptors. Stream interceptors will be adapted to unary RPCs via StreamServerInterceptorToUnary.
func WithServerInterceptorsAsUnary(interceptors ...interface{}) grpc.ServerOption {
	return grpc.UnaryInterceptor(combineServerInterceptorsIntoUnary(interceptors))
}

func combineServerInterceptorsIntoUnary(interceptors []interface{}) grpc.UnaryServerInterceptor {
	var unary []grpc.UnaryServerInterceptor
	var streams []grpc.StreamServerInterceptor
	for _, i := range interceptors {
		switch i := i.(type) {
		case grpc.UnaryServerInterceptor:
			if len(streams) > 0 {
				u := StreamServerInterceptorToUnary(CombineStreamServerInterceptors(streams...))
				unary = append(unary, u)
				streams = nil
			}
			unary = append(unary, i)
		case grpc.StreamServerInterceptor:
			streams = append(streams, i)
		default:
			panic(fmt.Sprintf("Given argument is neither a unary nor stream client interceptor: %v", reflect.TypeOf(i)))
		}
	}
	if len(streams) > 0 {
		u := StreamServerInterceptorToUnary(CombineStreamServerInterceptors(streams...))
		unary = append(unary, u)
	}
	return CombineUnaryServerInterceptors(unary...)
}

// WithServerInterceptorsStream returns a ServerOption, for configuring a GRPC server that uses the
// given interceptors with streaming RPC methods.
func WithStreamServerInterceptors(interceptors ...grpc.StreamServerInterceptor) grpc.ServerOption {
	return grpc.StreamInterceptor(CombineStreamServerInterceptors(interceptors...))
}

// CombineUnaryServerInterceptors combines the given interceptors, in order, and returns a single
// resulting interceptor. The first interceptor given will be the first one invoked. It is passed
// a handler that will actually invoke the next interceptor in the slice, and so on. The final
// interceptor given will be passed the original handler.
func CombineUnaryServerInterceptors(interceptors ...grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	if len(interceptors) == 0 {
		return nil
	}
	if len(interceptors) == 1 {
		return interceptors[0]
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
	if len(interceptors) == 1 {
		return interceptors[0]
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
		if err != nil {
			return nil, err
		}
		err = interceptor(info.Server, stream, &grpc.StreamServerInfo{FullMethod: info.FullMethod}, streamHandler)
		if err != nil {
			return nil, err
		}
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
