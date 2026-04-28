package producer

import (
	"context"
	"io"
	"strings"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/client-go/kubernetes/fake"

	dbspunstructured "github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

var _ = Describe("LogProducer", func() {
	It("requires Client", func() {
		_, err := NewLogProducer(LogConfig{
			Name:      "x",
			PodName:   "p",
			InputName: "t",
			Runtime:   dbspruntime.NewRuntime(logr.Discard()),
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("client"))
	})

	It("requires Runtime", func() {
		_, err := NewLogProducer(LogConfig{
			Client:    fake.NewSimpleClientset(),
			Name:      "x",
			PodName:   "p",
			InputName: "t",
		})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("runtime"))
	})

	It("defaults namespace to 'default'", func() {
		rt := dbspruntime.NewRuntime(logr.Discard())
		p, err := NewLogProducer(LogConfig{
			Client:    fake.NewSimpleClientset(),
			Name:      "ns-test",
			PodName:   "pod",
			InputName: "t",
			Runtime:   rt,
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(p.namespace).To(Equal("default"))
	})

	It("returns nil on context cancellation", func() {
		rt := dbspruntime.NewRuntime(logr.Discard())
		p, err := NewLogProducer(LogConfig{
			Client:    fake.NewSimpleClientset(),
			Name:      "cancel-test",
			PodName:   "pod",
			Namespace: "ns",
			InputName: "logs",
			Runtime:   rt,
			Logger:    logr.Discard(),
		})
		Expect(err).NotTo(HaveOccurred())

		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- p.Start(ctx) }()
		cancel()
		Eventually(errCh, 2*time.Second).Should(Receive(BeNil()))
	})

	It("emits one event per log line via stream injection", func() {
		rt := dbspruntime.NewRuntime(logr.Discard())
		sub := rt.NewSubscriber()
		sub.Subscribe("logs")

		// Provide a custom stream func to inject lines without a real kubelet.
		lines := "first line\nsecond line\n"
		streamFn := func(ctx context.Context) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader(lines)), nil
		}

		p, err := newLogProducerWithStream(LogConfig{
			Client:    fake.NewSimpleClientset(),
			Name:      "stream-test",
			PodName:   "pod",
			Namespace: "default",
			InputName: "logs",
			Runtime:   rt,
			Logger:    logr.Discard(),
		}, streamFn)
		Expect(err).NotTo(HaveOccurred())

		Expect(rt.Add(p)).To(Succeed())
		ctx, cancel := context.WithCancel(context.Background())
		errCh := make(chan error, 1)
		go func() { errCh <- rt.Start(ctx) }()

		var received []string
		Eventually(func() bool {
			select {
			case evt := <-sub.GetChannel():
				for _, e := range evt.Data.Entries() {
					if u, ok := e.Document.(*dbspunstructured.Unstructured); ok {
						if v, ok := u.Fields()["line"]; ok {
							received = append(received, v.(string))
						}
					}
				}
			default:
			}
			return len(received) >= 2
		}, 2*time.Second, 10*time.Millisecond).Should(BeTrue())

		Expect(received).To(ContainElements("first line", "second line"))

		cancel()
		Eventually(errCh, time.Second).Should(Receive(BeNil()))
	})
})
