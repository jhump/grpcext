package grpcext

import (
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// RetryConfig defines the parameters for when and how often an RPC is retried
// by the UnaryRetries and StreamingRetries interceptors.
type RetryConfig struct {
	// The maximum number of attempts to make for a single RPC operation. A
	// value of zero means that no explicit limit is set.
	MaxAttempts       uint

	// The minimum amount of time to elapse between attempts. This is the
	// initial delay, before the first retry. After each attempt the delay is
	// multiplied by DelayMultiplier, but capped at MaxRetryDelay. This value
	// must be positive.
	MinRetryDelay     time.Duration

	// The maximum duration, for growing the retry delay by DelayMultiplier.
	// This value may be zero to indicate no maximum duration. If non-zero, it
	// must be greater than or equal to MinRetryDelay
	MaxRetryDelay     time.Duration

	// The scale used to increase the delay between attempts for back-off. A
	// value of 1.0 will keep a constant retry delay. This value must not be
	// negative. A value of less than 1.0 will be assumed to mean 1.0.
	DelayMultiplier   float32

	// A function that decides whether or not an RPC is retry-able. If nil, only
	// connection errors prior to any request message being sent will be
	// retried.
	CanRetry          func(context.Context, error) bool

	// A function that, given a fully-qualified method name, returns the time
	// after which a "hedge" attempt may be sent. When hedging, a retry attempt
	// is made even before the previous attempt fails, triggered only by this
	// timeout having elapsed. The first successful result of the outstanding
	// attempts is used as the operation result.
	//
	// For streaming operations, the elapsed time is measured starting with a
	// call to either stream.Header() or stream.RecvMsg(). When a stream is
	// hedged, the first stream that is successful and either completes or
	// exceeds the configured max response buffer will be used.
	//
	// When an attempt is used as the operation result, any other hedges that
	// are still outstanding will be cancelled.
	//
	// If nil, no hedging is done for any RPC. If non-nil and returns the zero
	// duration for an RPC, no hedging will be done for that RPC.
	AttemptTimeout    func(context.Context, string) time.Duration
}

type StreamRetryConfig struct {
	RetryConfig

	// The maximum number of request bytes to buffer, for streaming requests. If
	// the request stream exceeds this size, it cannot be retried.
	//
	// A value of zero disables retries for RPCs with request streams. This
	// value cannot be negative.
	MaxRequestBuffer  int

	// The maximum number of response bytes to buffer, for streaming responses.
	// If the response stream exceeds this size, it cannot be retried.
	// Furthermore, headers and response messages will be buffered (and not
	// delivered to the application) until this buffer limit is reached. If an
	// attempt fails before reaching this limit, the buffered response messages
	// are discarded and the operation is retried.
	//
	// A value of zero disables retries for RPCs with request streams. This
	// value cannot be negative.
	MaxResponseBuffer int
}

func UnaryRetries(conf RetryConfig) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var (
			numAttempts int32
			lastHeaders, lastTrailers metadata.MD
			lastErr error
		)

		var attemptTimeout time.Duration
		if conf.AttemptTimeout != nil {
			attemptTimeout = conf.AttemptTimeout(ctx, method)
		}
		if attemptTimeout.Nanoseconds() > 0 {
			if attemptTimeout.Nanoseconds() < conf.MaxRetryDelay.Nanoseconds() {
				attemptTimeout = conf.MinRetryDelay
			}
		}

		// TODO: implement me

	}
}

type result struct {
	header
}

func StreamRetries(conf StreamRetryConfig) grpc.StreamClientInterceptor {
	// TODO: implement me
	return nil
}
