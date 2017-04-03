// Package grpcext contains useful extensions to GRPC.
//
// At the moment, the contents focus just on interceptors. GRPC allows applications to register
// up to one interceptor for unary RPCs and up to one interceptor for streaming RPCs. But it does
// provide an easy way to unify the two -- when you need an interceptor that handles both unary
// and streaming RPCs. Also, it only allows for exactly one interceptor, not multiple. So this
// package has several new things to address that:
//
// 1. Unified interceptor interfaces: this package contains two new interfaces, ServerInterceptor
//    and ClientInterceptor, which can be implemented to intercept both unary and streaming RPCs.
//    Adapter functions are also provided, to easily adapt one or more such interceptors to the
//    relevant GRPC interface (e.g. UnaryServerInterceptor, StreamServerInterceptor,
//    UnaryClientInterceptor, or StreamClientInterceptor).
// 2. This package also includes generally useful functions for combining interceptors. So if you
//    have multiple unary interceptors or multiple stream interceptors, these functions allow you
//    to combine them into one, so that you can configure the GRPC client or server with it.
// 3. Finally, this package contains an adapter so that a stream interceptor (implementing the
//    GRPC interface StreamServerInterceptor or StreamClientInterceptor) can be used to intercept
//    unary RPCs. Generally, it is advised to instead use the new interfaces provided in this
//    package as they are much simpler to adapt.
package grpcext
