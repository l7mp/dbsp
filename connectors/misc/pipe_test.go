package misc

import (
	"context"
	"time"

	"github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pipe connector", func() {
	It("forwards input from producer channel", func() {
		in := make(chan runtime.Event, 1)
		p := NewPipeProducer(in)

		errCh := make(chan error, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gotCh := make(chan runtime.Event, 1)
		p.SetPublisher(runtime.PublishFunc(func(in runtime.Event) error {
			gotCh <- in
			return nil
		}))

		go func() { errCh <- p.Start(ctx) }()

		want := runtime.Event{Name: "in", Data: zset.New()}
		in <- want

		var got runtime.Event
		Eventually(gotCh, time.Second).Should(Receive(&got))
		Expect(got.Name).To(Equal(want.Name))
		Expect(got.Data.Equal(want.Data)).To(BeTrue())

		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	})

	It("forwards output to consumer channel", func() {

		out := make(chan runtime.Event, 1)
		c := NewPipeConsumer(out)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		want := runtime.Event{Name: "out", Data: zset.New()}
		Expect(c.Consume(ctx, want)).To(Succeed())

		var got runtime.Event
		Eventually(out, time.Second).Should(Receive(&got))
		Expect(got.Name).To(Equal(want.Name))
		Expect(got.Data.Equal(want.Data)).To(BeTrue())

	})
})
