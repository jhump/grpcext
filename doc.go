// Package grpcext contains useful extensions to GRPC.
//
// At the moment, the contents focus just on interceptors. GRPC allows applications to register
// up to one interceptor for unary RPCs and up to one interceptor for streaming RPCs. But it does
// not provide any way to unify the two -- when you need an interceptor that handles both unary
// and streaming RPCs. Also, it only allows for exactly one interceptor, not multiple. So this
// package has some new things to address that:
//
// 1. This package contains adapters so that stream interceptors can be used to intercept unary
//    RPCs. This allows an interceptor to be written once and used in all contexts. Note that the
//    other direction (adapting a unary interceptor for a streaming RPC) is not really possible.
// 2. This package includes generally useful functions for combining interceptors. So if you have
//    multiple unary interceptors or multiple stream interceptors, these functions allow you to
//    combine them into one, so that you can configure the GRPC client or server with it.
package grpcext
