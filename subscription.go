package broker

import (
	types "gopkg.in/gomisc/types.v1"
)

// Subscription - event subscription
type subscription[T types.Ordered] struct {
	name string
	chEv chan Event[T]
}

// NewSubscription - event subscribe constructor
func NewSubscription[T types.Ordered](name string, sz int) Subscription[T] {
	sub := &subscription[T]{
		name: name,
		chEv: make(chan Event[T], sz),
	}
	return sub
}

func (s *subscription[T]) GetName() string {
	return s.name
}

func (s *subscription[T]) ChanEv() chan Event[T] {
	return s.chEv
}
