package grpcext

import (
	"fmt"
	"io"
	"net"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/golang/protobuf/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/grpc_testing"
)

var (
	actualClientStream *grpc.StreamClientInterceptor
	actualClientUnary *grpc.UnaryClientInterceptor

	actualServerStream *grpc.StreamServerInterceptor
	actualServerUnary *grpc.UnaryServerInterceptor

	stub grpc_testing.TestServiceClient
)

func TestMain(m *testing.M) {
	code := 1
	defer func() {
		p := recover()
		if p != nil {
			fmt.Fprintf(os.Stderr, "PANIC: %v\n", p)
		}
		os.Exit(code)
	}()

	// Start up a server on an ephemeral port
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(fmt.Sprintf("Failed to listen to port: %s", err.Error()))
	}
	svr := grpc.NewServer(grpc.StreamInterceptor(swappingStreamServerInterceptor), grpc.UnaryInterceptor(swappingUnaryServerInterceptor))
	grpc_testing.RegisterTestServiceServer(svr, TestService{})
	go svr.Serve(l)
	defer svr.Stop()

	// Start up client that talks to the same port
	cc, err := grpc.Dial(l.Addr().String(), grpc.WithInsecure(), grpc.WithStreamInterceptor(swappingStreamClientInterceptor), grpc.WithUnaryInterceptor(swappingUnaryClientInterceptor))
	if err != nil {
		panic(fmt.Sprintf("Failed to create client to %s: %s", l.Addr().String(), err.Error()))
	}
	defer cc.Close()

	stub = grpc_testing.NewTestServiceClient(cc)

	code = m.Run()
}

func TestClientInterceptors(t *testing.T) {
	t.Run("combineThenConvert", asTest(
		func(ints []ClientInterceptor) grpc.StreamClientInterceptor {
			return ClientInterceptorAsGrpcStream(combineClientInterceptors(ints))
		},
		func(ints []ClientInterceptor) grpc.UnaryClientInterceptor {
			return ClientInterceptorAsGrpcUnary(combineClientInterceptors(ints))
		},
		nil, nil))
	t.Run("combineThenConvertFromStream", asTest(
		nil,
		func(ints []ClientInterceptor) grpc.UnaryClientInterceptor {
			return StreamClientInterceptorToUnary(ClientInterceptorAsGrpcStream(combineClientInterceptors(ints)))
		},
		nil, nil))
	t.Run("convertThenCombine", asTest(
		func(ints []ClientInterceptor) grpc.StreamClientInterceptor {
			grpcInts := make([]grpc.StreamClientInterceptor, len(ints))
			for i, in := range ints {
				grpcInts[i] = ClientInterceptorAsGrpcStream(in)
			}
			return CombineStreamClientInterceptors(grpcInts...)
		},
		func(ints []ClientInterceptor) grpc.UnaryClientInterceptor {
			grpcInts := make([]grpc.UnaryClientInterceptor, len(ints))
			for i, in := range ints {
				grpcInts[i] = ClientInterceptorAsGrpcUnary(in)
			}
			return CombineUnaryClientInterceptors(grpcInts...)
		},
		nil, nil))
	t.Run("convertThenCombineFromStream", asTest(
		nil,
		func(ints []ClientInterceptor) grpc.UnaryClientInterceptor {
			grpcInts := make([]grpc.StreamClientInterceptor, len(ints))
			for i, in := range ints {
				grpcInts[i] = ClientInterceptorAsGrpcStream(in)
			}
			return StreamClientInterceptorToUnary(CombineStreamClientInterceptors(grpcInts...))
		},
		nil, nil))
	t.Run("convertFromStreamThenCombine", asTest(
		nil,
		func(ints []ClientInterceptor) grpc.UnaryClientInterceptor {
			grpcInts := make([]grpc.UnaryClientInterceptor, len(ints))
			for i, in := range ints {
				grpcInts[i] = StreamClientInterceptorToUnary(ClientInterceptorAsGrpcStream(in))
			}
			return CombineUnaryClientInterceptors(grpcInts...)
		},
		nil, nil))
}

func TestServerInterceptors(t *testing.T) {
	t.Run("combineThenConvert", asTest(
		nil,  nil,
		func(ints []ServerInterceptor) grpc.StreamServerInterceptor {
			return ServerInterceptorAsGrpcStream(combineServerInterceptors(ints))
		},
		func(ints []ServerInterceptor) grpc.UnaryServerInterceptor {
			return ServerInterceptorAsGrpcUnary(combineServerInterceptors(ints))
		}))
	t.Run("combineThenConvertFromStream", asTest(
		nil,  nil, nil,
		func(ints []ServerInterceptor) grpc.UnaryServerInterceptor {
			return StreamServerInterceptorToUnary(ServerInterceptorAsGrpcStream(combineServerInterceptors(ints)))
		}))
	t.Run("convertThenCombine", asTest(
		nil,  nil,
		func(ints []ServerInterceptor) grpc.StreamServerInterceptor {
			grpcInts := make([]grpc.StreamServerInterceptor, len(ints))
			for i, in := range ints {
				grpcInts[i] = ServerInterceptorAsGrpcStream(in)
			}
			return CombineStreamServerInterceptors(grpcInts...)
		},
		func(ints []ServerInterceptor) grpc.UnaryServerInterceptor {
			grpcInts := make([]grpc.UnaryServerInterceptor, len(ints))
			for i, in := range ints {
				grpcInts[i] = ServerInterceptorAsGrpcUnary(in)
			}
			return CombineUnaryServerInterceptors(grpcInts...)
		}))
	t.Run("convertThenCombineFromStream", asTest(
		nil,  nil, nil,
		func(ints []ServerInterceptor) grpc.UnaryServerInterceptor {
			grpcInts := make([]grpc.StreamServerInterceptor, len(ints))
			for i, in := range ints {
				grpcInts[i] = ServerInterceptorAsGrpcStream(in)
			}
			return StreamServerInterceptorToUnary(CombineStreamServerInterceptors(grpcInts...))
		}))
	t.Run("convertFromStreamThenCombine", asTest(
		nil,  nil, nil,
		func(ints []ServerInterceptor) grpc.UnaryServerInterceptor {
			grpcInts := make([]grpc.UnaryServerInterceptor, len(ints))
			for i, in := range ints {
				grpcInts[i] = StreamServerInterceptorToUnary(ServerInterceptorAsGrpcStream(in))
			}
			return CombineUnaryServerInterceptors(grpcInts...)
		}))
}

var payload = &grpc_testing.Payload{
	Type: grpc_testing.PayloadType_RANDOM.Enum(),
	Body: []byte{ 3, 14, 159, 2, 65, 35, 9 },
}

func asTest(
	clientStream func([]ClientInterceptor) grpc.StreamClientInterceptor,
	clientUnary func([]ClientInterceptor) grpc.UnaryClientInterceptor,
	serverStream func([]ServerInterceptor) grpc.StreamServerInterceptor,
	serverUnary func([]ServerInterceptor) grpc.UnaryServerInterceptor) func(t *testing.T) {

	return func(t *testing.T) {
		defer resetInterceptors()

		var lock sync.Mutex
		var ints, reqs, resps []int

		// unary method
		if clientUnary != nil || serverUnary != nil {
			setInterceptors(unaryMethod, &lock, &ints, &reqs, &resps, clientStream, clientUnary, serverStream, serverUnary)
			unResp, err := stub.UnaryCall(context.Background(), &grpc_testing.SimpleRequest{Payload: payload})
			ok(t, err, "RPC failed")
			assert(t, proto.Equal(payload, unResp.Payload), "Incorrect response received!\nExpected %v\nGot %v", payload, unResp.Payload)
			checkCounts(t, &lock, &ints, &reqs, &resps, clientUnary != nil, serverUnary != nil, 1, 1)
			// reset
			ints = nil
			reqs = nil
			resps = nil
		}

		// client streaming method
		if clientStream != nil || serverStream != nil {
			setInterceptors(clientStreamMethod, &lock, &ints, &reqs, &resps, clientStream, clientUnary, serverStream, serverUnary)
			cs, err := stub.StreamingInputCall(context.Background())
			ok(t, err, "RPC failed")
			strInReq := &grpc_testing.StreamingInputCallRequest{Payload: payload}
			for i := 0; i < 3; i++ {
				err = cs.Send(strInReq)
				ok(t, err, "Sending message failed")
			}
			strInResp, err := cs.CloseAndRecv()
			ok(t, err, "Receiving response failed")
			expSize := int32(3*len(payload.Body))
			eq(t, expSize, strInResp.GetAggregatedPayloadSize(), "Incorrect response received!\nExpected %v\nGot %v", expSize, strInResp.AggregatedPayloadSize)
			checkCounts(t, &lock, &ints, &reqs, &resps, clientStream != nil, serverStream != nil, 3, 1)
			// reset
			ints = nil
			reqs = nil
			resps = nil
		}

		// server streaming method
		if clientStream != nil || serverStream != nil {
			setInterceptors(serverStreamMethod, &lock, &ints, &reqs, &resps, clientStream, clientUnary, serverStream, serverUnary)
			strOutReq := &grpc_testing.StreamingOutputCallRequest{
				Payload: payload,
				ResponseParameters: []*grpc_testing.ResponseParameters{
					{}, {}, {}, // three entries means we'll get back three responses
				},
			}
			ss, err := stub.StreamingOutputCall(context.Background(), strOutReq)
			ok(t, err, "RPC failed")
			for i := 0; i < 3; i++ {
				resp, err := ss.Recv()
				ok(t, err, "Receiving message failed")
				assert(t, proto.Equal(payload, resp.Payload), "Incorrect response received!\nExpected %v\nGot %v", payload, resp.Payload)
			}
			_, err = ss.Recv()
			eq(t, io.EOF, err, "Incorrect number of messages in response")
			checkCounts(t, &lock, &ints, &reqs, &resps, clientStream != nil, serverStream != nil, 1, 3)
			// reset
			ints = nil
			reqs = nil
			resps = nil
		}

		// bidi streaming method
		if clientStream != nil || serverStream != nil {
			setInterceptors(bidiStreamMethod, &lock, &ints, &reqs, &resps, clientStream, clientUnary, serverStream, serverUnary)
			bds, err := stub.FullDuplexCall(context.Background())
			ok(t, err, "RPC failed")
			strOutReq := &grpc_testing.StreamingOutputCallRequest{Payload: payload}
			for i := 0; i < 3; i++ {
				err := bds.Send(strOutReq)
				ok(t, err, "Sending message failed")
				resp, err := bds.Recv()
				ok(t, err, "Receiving message failed")
				assert(t, proto.Equal(payload, resp.Payload), "Incorrect response received!\nExpected %v\nGot %v", payload, resp.Payload)
			}
			err = bds.CloseSend()
			ok(t, err, "Closing stream failed")
			_, err = bds.Recv()
			eq(t, io.EOF, err, "Incorrect number of messages in response")
			checkCounts(t, &lock, &ints, &reqs, &resps, clientStream != nil, serverStream != nil, 3, 3)
			// reset
			ints = nil
			reqs = nil
			resps = nil
		}
	}
}

func setInterceptors(desc *methodDesc, lock *sync.Mutex, ints, reqs, resps *[]int,
	clientStream func([]ClientInterceptor) grpc.StreamClientInterceptor,
	clientUnary func([]ClientInterceptor) grpc.UnaryClientInterceptor,
	serverStream func([]ServerInterceptor) grpc.StreamServerInterceptor,
	serverUnary func([]ServerInterceptor) grpc.UnaryServerInterceptor) {

	ci := makeClientInterceptors(desc, lock, ints, reqs, resps)
	var csi grpc.StreamClientInterceptor
	if clientStream != nil {
		csi = clientStream(ci)
	}
	var cui grpc.UnaryClientInterceptor
	if clientUnary != nil {
		cui = clientUnary(ci)
	}
	si := makeServerInterceptors(desc, lock, ints, reqs, resps)
	var ssi grpc.StreamServerInterceptor
	if serverStream != nil {
		ssi = serverStream(si)
	}
	var sui grpc.UnaryServerInterceptor
	if serverUnary != nil {
		sui = serverUnary(si)
	}

	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&actualClientStream)), unsafe.Pointer(&csi))
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&actualClientUnary)), unsafe.Pointer(&cui))
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&actualServerStream)), unsafe.Pointer(&ssi))
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&actualServerUnary)), unsafe.Pointer(&sui))
}

const numInterceptors = 3

func makeClientInterceptors(desc *methodDesc, lock *sync.Mutex, ints, reqs, resps *[]int) []ClientInterceptor {
	var ret []ClientInterceptor
	for i := 0; i < numInterceptors ; i++ {
		ret = append(ret, makeClientInterceptor(desc, i, lock, ints, reqs, resps))
	}
	return ret
}

func makeServerInterceptors(desc *methodDesc, lock *sync.Mutex, ints, reqs, resps *[]int) []ServerInterceptor {
	var ret []ServerInterceptor
	for i := 0; i < numInterceptors ; i++ {
		ret = append(ret, makeServerInterceptor(desc, i+numInterceptors, lock, ints, reqs, resps))
	}
	return ret
}

func resetInterceptors() {
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&actualClientStream)), nil)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&actualClientUnary)), nil)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&actualServerStream)), nil)
	atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&actualServerUnary)), nil)
}

func checkCounts(t *testing.T, lock *sync.Mutex, ints, reqs, resps *[]int, incClient, incServer bool, numReqs, numResps int) {
	lock.Lock()
	defer lock.Unlock()

	var start, end int
	if incClient {
		start = 0
	} else {
		start = 3
	}
	if incServer {
		end = 6
	} else {
		end = 3
	}

	// interceptors invoked in order
	inti := 0
	for i := start; i < end; i++ {
		eq(t, i, (*ints)[inti], "Wrong interceptor")
		inti++
	}
	// request message observers invoked in order, for each message
	reqi := 0
	for j := 0; j < numReqs; j++ {
		for i := start; i < end; i++ {
			eq(t, i * 10, (*reqs)[reqi], "Wrong message observer")
			reqi++
		}
	}
	// response message observers invoked in opposite order
	respi := 0
	for j := 0; j < numResps; j++ {
		for i := end - 1; i >= start; i-- {
			eq(t, i * 100, (*resps)[respi], "Wrong message observer")
			respi++
		}
	}
}

type methodDesc struct {
	name         string
	clientStream bool
	serverStream bool
	reqType      reflect.Type
	respType     reflect.Type
}

var unaryMethod = &methodDesc{
	name:         "/grpc.testing.TestService/UnaryCall",
	clientStream: false,
	serverStream: false,
	reqType:      reflect.TypeOf((*grpc_testing.SimpleRequest)(nil)),
	respType:     reflect.TypeOf((*grpc_testing.SimpleResponse)(nil)),
}

var clientStreamMethod = &methodDesc{
	name:         "/grpc.testing.TestService/StreamingInputCall",
	clientStream: true,
	serverStream: false,
	reqType:      reflect.TypeOf((*grpc_testing.StreamingInputCallRequest)(nil)),
	respType:     reflect.TypeOf((*grpc_testing.StreamingInputCallResponse)(nil)),
}

var serverStreamMethod = &methodDesc{
	name:         "/grpc.testing.TestService/StreamingOutputCall",
	clientStream: false,
	serverStream: true,
	reqType:      reflect.TypeOf((*grpc_testing.StreamingOutputCallRequest)(nil)),
	respType:     reflect.TypeOf((*grpc_testing.StreamingOutputCallResponse)(nil)),
}

var bidiStreamMethod = &methodDesc{
	name:         "/grpc.testing.TestService/FullDuplexCall",
	clientStream: true,
	serverStream: true,
	reqType:      reflect.TypeOf((*grpc_testing.StreamingOutputCallRequest)(nil)),
	respType:     reflect.TypeOf((*grpc_testing.StreamingOutputCallResponse)(nil)),
}

func makeClientInterceptor(desc *methodDesc, index int, lock *sync.Mutex, ints, reqs, resps *[]int) ClientInterceptor {
	return func(ctx context.Context, cc *grpc.ClientConn, info *grpc.StreamServerInfo, proceed Invocation) error {
		return intercept("client", ctx, info, proceed, desc, index, lock, ints, reqs, resps)
	}
}

func makeServerInterceptor(desc *methodDesc, index int, lock *sync.Mutex, ints, reqs, resps *[]int) ServerInterceptor {
	return func(ctx context.Context, srv interface{}, info *grpc.StreamServerInfo, proceed Invocation) error {
		return intercept("server", ctx, info, proceed, desc, index, lock, ints, reqs, resps)
	}
}

func intercept(where string, ctx context.Context, info *grpc.StreamServerInfo, proceed Invocation, desc *methodDesc, index int, lock *sync.Mutex, ints, reqs, resps *[]int) error {
	if info.FullMethod != desc.name {
		return fmt.Errorf("Wrong method in %s; got %q, expected %q", where, info.FullMethod, desc.name)
	}
	if info.IsClientStream != desc.clientStream {
		return fmt.Errorf("Wrong client streaming mode in %s; got %q, expected %q", where, info.IsClientStream, desc.clientStream)
	}
	if info.IsServerStream != desc.serverStream {
		return fmt.Errorf("Wrong server streaming mode in %s; got %q, expected %q", where, info.IsServerStream, desc.serverStream)
	}
	lock.Lock()
	*ints = append(*ints, index)
	lock.Unlock()
	reqObs := makeObserver(where, desc.reqType, index * 10, lock, reqs)
	respObs := makeObserver(where, desc.respType, index * 100, lock, resps)
	return proceed(ctx, reqObs, respObs)
}

func makeObserver(where string, msgType reflect.Type, index int, lock *sync.Mutex, msgs *[]int) MessageObserver {
	return func(m interface{}) error {
		if reflect.TypeOf(m) != msgType {
			return fmt.Errorf("Wrong message type in %s; got %q, expected %q", where, reflect.TypeOf(m), msgType)
		}
		lock.Lock()
		*msgs = append(*msgs, index)
		lock.Unlock()
		return nil
	}
}

func swappingStreamClientInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	pi := (*grpc.StreamClientInterceptor)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&actualClientStream))))
	if pi == nil || *pi == nil {
		return streamer(ctx, desc, cc, method, opts...)
	} else {
		i := *pi
		return i(ctx, desc, cc, method, streamer, opts...)
	}
}

func swappingUnaryClientInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	pi := (*grpc.UnaryClientInterceptor)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&actualClientUnary))))
	if pi == nil || *pi == nil {
		return invoker(ctx, method, req, reply, cc, opts...)
	} else {
		i := *pi
		return i(ctx, method, req, reply, cc, invoker, opts...)
	}
}

func swappingStreamServerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	pi := (*grpc.StreamServerInterceptor)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&actualServerStream))))
	if pi == nil || *pi == nil {
		return handler(srv, ss)
	} else {
		i := *pi
		return i(srv, ss, info, handler)
	}
}

func swappingUnaryServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	pi := (*grpc.UnaryServerInterceptor)(atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&actualServerUnary))))
	if pi == nil || *pi == nil {
		return handler(ctx, req)
	} else {
		i := *pi
		return i(ctx, req, info, handler)
	}
}

// very simple test service that just echos back request payloads
type TestService struct {}

func (_ TestService) EmptyCall(context.Context, *grpc_testing.Empty) (*grpc_testing.Empty, error) {
	return &grpc_testing.Empty{}, nil
}

func (_ TestService) UnaryCall(_ context.Context, req *grpc_testing.SimpleRequest) (*grpc_testing.SimpleResponse, error) {
	return &grpc_testing.SimpleResponse{
		Payload: req.Payload,
	}, nil
}

func (_ TestService) StreamingOutputCall(req *grpc_testing.StreamingOutputCallRequest, ss grpc_testing.TestService_StreamingOutputCallServer) error {
	for i := 0; i < len(req.GetResponseParameters()); i++ {
		ss.Send(&grpc_testing.StreamingOutputCallResponse{
			Payload: req.Payload,
		})
	}
	return nil
}

func (_ TestService) StreamingInputCall(ss grpc_testing.TestService_StreamingInputCallServer) error {
	sz := 0
	for {
		req, err := ss.Recv()
		if err == io.EOF { break }
		if err != nil { return err }
		sz += len(req.Payload.GetBody())
	}
	return ss.SendAndClose(&grpc_testing.StreamingInputCallResponse{
		AggregatedPayloadSize: proto.Int(sz),
	})
}

func (_ TestService) FullDuplexCall(ss grpc_testing.TestService_FullDuplexCallServer) error {
	for {
		req, err := ss.Recv()
		if err == io.EOF { return nil }
		if err != nil { return err }

		err = ss.Send(&grpc_testing.StreamingOutputCallResponse{
			Payload: req.Payload,
		})
		if err != nil { return err }
	}
}

func (_ TestService) HalfDuplexCall(ss grpc_testing.TestService_HalfDuplexCallServer) error {
	var data []*grpc_testing.Payload
	for {
		req, err := ss.Recv()
		if err == io.EOF { break }
		if err != nil { return err }
		data = append(data, req.Payload)
	}

	for _, d := range data {
		err := ss.Send(&grpc_testing.StreamingOutputCallResponse{
			Payload: d,
		})
		if err != nil { return err }
	}
	return nil
}

// simple assertion helpers

func ok(t *testing.T, err error, failureMsgFmt string, msgFmtArgs ...interface{}) {
	assert(t, err == nil, failureMsgFmt, msgFmtArgs...)
}

func eq(t *testing.T, expected, actual interface{}, failureMsgFmt string, msgFmtArgs ...interface{}) {
	assert(t, expected == actual, failureMsgFmt, msgFmtArgs...)
}

func assert(t *testing.T, condition bool, failureMsgFmt string, msgFmtArgs ...interface{}) {
	if !condition {
		t.Errorf(failureMsgFmt, msgFmtArgs...)
	}
}