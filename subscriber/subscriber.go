package subscriber

import (
	"fmt"
	"sync"

	types "gopkg.in/gomisc/types.v1"

	"gopkg.in/gomisc/broker.v1"
)

type (
	Worker[T types.Ordered] func(queue <-chan broker.Event[T])

	Subscriber[T types.Ordered] struct {
		log    slog.Logger
		broker broker.Broker[T]
		subs   []broker.Subscription[T]
		wg     sync.WaitGroup
	}
)

func Run[T types.Ordered](
	log slog.Logger,
	b broker.Broker[T],
	workers int,
	worker Worker[T],
	kinds ...fmt.Stringer,
) *Subscriber[T] {
	subscriber := &Subscriber[T]{
		log:    log,
		broker: b,
		subs:   make([]broker.Subscription[T], len(kinds)),
	}

	for i := 0; i < len(kinds); i++ {
		name := kinds[i].String()

		sub, err := subscriber.broker.Subscribe(name)
		if err != nil {
			log.Error(errors.Ctx().Str("kind", name).Wrap(err, "subscribe"))
			continue
		}

		subscriber.subs = append(subscriber.subs, sub)

		for w := 0; w < workers; w++ {
			subscriber.wg.Add(1)

			go func() {
				worker(sub.ChanEv())
				subscriber.wg.Done()
			}()
		}
	}

	return subscriber
}

func (s *Subscriber[T]) Stop() {
	for i := 0; i < len(s.subs); i++ {
		if err := s.broker.Unsubscribe(s.subs[i]); err != nil {
			s.log.Error("unsubscribe", err)
		}
	}
}

func (s *Subscriber[T]) Wait() {
	s.wg.Wait()
}
