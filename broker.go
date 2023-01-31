package broker

import (
	"context"
	"os"
	"sync"

	"git.eth4.dev/golibs/errors"
	"git.eth4.dev/golibs/fields"
	"git.eth4.dev/golibs/slog"
	"git.eth4.dev/golibs/types"
)

const ErrBrokerIsStopped = errors.Const("operation on stopped broker")

type broker[T types.Ordered] struct {
	ctx context.Context
	log slog.Logger

	eventQueueSize int
	pubCh          chan Event[T]
	subCh          chan Subscription[T]
	unsubCh        chan Subscription[T]

	mu      sync.RWMutex
	stopped bool
}

// New - simple broker constructor
func New[T types.Ordered](ctx context.Context, qsz int) Broker[T] {
	return &broker[T]{
		ctx:            ctx,
		log:            slog.MustFromContext(ctx).With(fields.Str("component", "broker")),
		eventQueueSize: qsz,
		pubCh:          make(chan Event[T], 1),
		subCh:          make(chan Subscription[T], 1),
		unsubCh:        make(chan Subscription[T], 1),
	}
}

func (b *broker[T]) Run(signals <-chan os.Signal, ready chan<- struct{}) (err error) {
	subs := map[string]map[chan Event[T]]struct{}{}

	close(ready)
	b.log.Info("started")

	for {
		select {
		case <-signals:
			b.log.Info("shutting down")

			if err = b.stop(); err != nil {
				return err
			}

			for name, closingSubs := range subs {
				for sub := range closingSubs {
					close(sub)
					b.log.Info("subscription closed", fields.Str("name", name))
				}
			}

			return nil
		case sub := <-b.subCh:
			name := sub.GetName()

			if _, ok := subs[name]; !ok {
				subs[name] = make(map[chan Event[T]]struct{})
			}

			subs[name][sub.ChanEv()] = struct{}{}
		case sub := <-b.unsubCh:
			close(sub.ChanEv())
			delete(subs, sub.GetName())
		case event := <-b.pubCh:
			for subscriber := range subs[event.SubName()] {
				subscriber <- event
			}
		}
	}
}

func (b *broker[T]) Subscribe(name string) (sub Subscription[T], err error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.stopped {
		s := NewSubscription[T](name, b.eventQueueSize)

		b.subCh <- s

		return s, nil
	}

	return nil, errors.Ctx().
		Str("operation", "subscribe").
		Just(ErrBrokerIsStopped)
}

func (b *broker[T]) Publish(ev Event[T]) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.stopped {
		b.pubCh <- ev

		return nil
	}

	return errors.Ctx().Str("operation", "publish").Just(ErrBrokerIsStopped)
}

func (b *broker[T]) Unsubscribe(sub Subscription[T]) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.stopped {
		b.unsubCh <- sub
		return nil
	}

	return errors.Ctx().Str("operation", "unsubscribe").Just(ErrBrokerIsStopped)
}

func (b *broker[T]) stop() error {
	b.mu.Lock()

	if b.stopped {
		b.mu.Unlock()

		return errors.Ctx().Str("operation", "stop").Just(ErrBrokerIsStopped)
	}

	b.stopped = true
	b.mu.Unlock()

	return nil
}