package broker

import (
	"git.eth4.dev/golibs/execs"
	"git.eth4.dev/golibs/types"
)

type (
	// Event service event
	Event[T types.Ordered] interface {
		SubName() string
		EventType() T
	}

	// Subscription - service event subscription
	Subscription[T types.Ordered] interface {
		GetName() string
		ChanEv() chan Event[T]
	}

	// Broker - simple  service event broker
	Broker[T types.Ordered] interface {
		execs.Runner
		Subscribe(name string) (sub Subscription[T], err error)
		Publish(ev Event[T]) error
		Unsubscribe(sub Subscription[T]) error
	}
)
