package misc

import (
	"context"
	"time"

	"github.com/l7mp/dbsp/dbsp/runtime"
	"github.com/l7mp/dbsp/dbsp/zset"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pipe connector", func() {
	It("forwards input from producer channel", func() {
		in := make(chan runtime.Input, 1)
		p := NewPipeProducer(in)

		errCh := make(chan error, 1)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		gotCh := make(chan runtime.Input, 1)
		p.SetInputHandler(func(_ context.Context, in runtime.Input) error {
			gotCh <- in
			return nil
		})

		go func() { errCh <- p.Start(ctx) }()

		want := runtime.Input{Name: "in", Data: zset.New()}
		in <- want

		var got runtime.Input
		Eventually(gotCh, time.Second).Should(Receive(&got))
		Expect(got.Name).To(Equal(want.Name))
		Expect(got.Data.Equal(want.Data)).To(BeTrue())

		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	})

	It("forwards output to consumer channel", func() {

		out := make(chan runtime.Output, 1)
		c := NewPipeConsumer(out)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		errCh := make(chan error, 1)
		go func() { errCh <- c.Start(ctx) }()

		want := runtime.Output{Name: "out", Data: zset.New()}
		Expect(c.Consume(ctx, want)).To(Succeed())

		var got runtime.Output
		Eventually(out, time.Second).Should(Receive(&got))
		Expect(got.Name).To(Equal(want.Name))
		Expect(got.Data.Equal(want.Data)).To(BeTrue())

		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	})
})
