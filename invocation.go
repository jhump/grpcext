package grpcext

import "golang.org/x/net/context"

// Invocation represents an RPC invocation, either unary or streaming. When invoked, an intercepted
// RPC will proceed (to either the next interceptor in the chain or to the actual server handler).
// The message observers are optional. Either or both can be nil if the caller is not interested in
// observing the request and/or response messages exchanged.
type Invocation func(ctx context.Context, reqObs MessageObserver, respObs MessageObserver) error

// MessageObserver is invoked when a message is sent or received during an RPC invocation.
type MessageObserver func(interface{}) error

func mergeResponseObservers(obs []MessageObserver) MessageObserver {
	if len(obs) == 0 {
		return nil
	}
	return func(m interface{}) error {
		// response messages are observed in reverse order
		for i := len(obs) - 1; i >= 0; i-- {
			ob := obs[i]
			if err := ob(m); err != nil {
				return err
			}
		}
		return nil
	}
}

func mergeRequestObservers(obs []MessageObserver) MessageObserver {
	if len(obs) == 0 {
		return nil
	}
	return func(m interface{}) error {
		for _, ob := range obs {
			if err := ob(m); err != nil {
				return err
			}
		}
		return nil
	}
}