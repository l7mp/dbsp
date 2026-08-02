package xds

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/go-logr/logr"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/l7mp/dbsp/engine/datamodel"
	"github.com/l7mp/dbsp/engine/datamodel/unstructured"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
	"github.com/l7mp/dbsp/engine/zset"
)

// collector subscribes to a topic and accumulates the net weight of every
// document by identity, so an ingested insert shows up, an update replaces, and
// a removal cancels out.
type collector struct {
	mu     sync.Mutex
	byHash map[string]collected
}

type collected struct {
	name   string
	weight int
}

func newCollector(ctx context.Context, rt *dbspruntime.Runtime, topic string) *collector {
	c := &collector{byHash: map[string]collected{}}
	sub := rt.NewSubscriber()
	sub.Subscribe(topic)
	go func() {
		ch := sub.GetChannel()
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-ch:
				if !ok {
					return
				}
				c.mu.Lock()
				for _, e := range ev.Data.Entries() {
					h := e.Document.Hash()
					cur := c.byHash[h]
					cur.name = docName(e.Document)
					cur.weight += int(e.Weight)
					c.byHash[h] = cur
				}
				c.mu.Unlock()
			}
		}
	}()
	return c
}

// liveNames returns the names of documents with net-positive weight.
func (c *collector) liveNames() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.byHash))
	for _, v := range c.byHash {
		if v.weight > 0 {
			out = append(out, v.name)
		}
	}
	sort.Strings(out)
	return out
}

func docName(d datamodel.Document) string {
	u, ok := d.(*unstructured.Unstructured)
	if !ok {
		return ""
	}
	f := u.Fields()
	if n, ok := f["name"].(string); ok {
		return n
	}
	if n, ok := f["clusterName"].(string); ok {
		return n
	}
	return ""
}

var _ = Describe("Producer ingest (against an in-process Server)", func() {
	var (
		rt       *dbspruntime.Runtime
		upstream *Server
		cancel   context.CancelFunc
		ctx      context.Context
		pub      dbspruntime.Publisher
	)

	cluster := func(name, typ string) *unstructured.Unstructured {
		return unstructured.New(map[string]any{"name": name, "type": typ})
	}

	BeforeEach(func() {
		rt = dbspruntime.NewRuntime(logr.Discard())
		var err error
		// The upstream is one of our own Servers, fed by an Updater on "up-src".
		upstream, err = NewServer(ServerConfig{Address: "127.0.0.1:0", Runtime: rt})
		Expect(err).NotTo(HaveOccurred())
		u, err := upstream.Updater(UpdaterConfig{Name: "up-cds", OutputName: "up-src", Type: "cds"})
		Expect(err).NotTo(HaveOccurred())
		pub = rt.NewPublisher()

		ctx, cancel = context.WithCancel(context.Background())
		go func() { defer GinkgoRecover(); Expect(upstream.Start(ctx)).To(Succeed()) }()
		go func() { defer GinkgoRecover(); Expect(u.Start(ctx)).To(Succeed()) }()
	})

	AfterEach(func() {
		if cancel != nil {
			cancel()
			cancel = nil
		}
	})

	feed := func(z zset.ZSet) {
		Expect(pub.Publish(dbspruntime.Event{Name: "up-src", Data: z})).To(Succeed())
	}

	It("Lister ingests the upstream snapshot over SotW", func() {
		add := zset.New()
		add.Insert(cluster("c1", "STATIC"), 1)
		add.Insert(cluster("c2", "EDS"), 1)
		feed(add)

		l, err := NewLister(ListerConfig{
			Name: "ingest-lister", InputName: "ingested", Type: "cds",
			Address: upstream.Address(), Runtime: rt,
		})
		Expect(err).NotTo(HaveOccurred())
		coll := newCollector(ctx, rt, "ingested")
		go func() { defer GinkgoRecover(); Expect(l.Start(ctx)).To(Succeed()) }()

		Eventually(coll.liveNames, 3*time.Second).Should(ConsistOf("c1", "c2"))
	})

	It("Watcher ingests the upstream incrementally over Delta", func() {
		add := zset.New()
		add.Insert(cluster("c1", "STATIC"), 1)
		feed(add)

		w, err := NewWatcher(WatcherConfig{
			Name: "ingest-watcher", InputName: "ingested-w", Type: "cds",
			Address: upstream.Address(), Runtime: rt,
		})
		Expect(err).NotTo(HaveOccurred())
		coll := newCollector(ctx, rt, "ingested-w")
		go func() { defer GinkgoRecover(); Expect(w.Start(ctx)).To(Succeed()) }()

		Eventually(coll.liveNames, 3*time.Second).Should(ConsistOf("c1"))

		// A subsequent removal upstream should cancel the ingested row.
		del := zset.New()
		del.Insert(cluster("c1", "STATIC"), -1)
		feed(del)

		Eventually(coll.liveNames, 3*time.Second).Should(BeEmpty())
	})

	It("Watcher survives an upstream restart, resyncing the state", func() {
		// A dedicated first upstream with its own lifetime, so it can be
		// killed under the watcher.
		ctx1, cancel1 := context.WithCancel(ctx)
		up1, err := NewServer(ServerConfig{Address: "127.0.0.1:0", Runtime: rt})
		Expect(err).NotTo(HaveOccurred())
		u1, err := up1.Updater(UpdaterConfig{Name: "up1-cds", OutputName: "up1-src", Type: "cds"})
		Expect(err).NotTo(HaveOccurred())
		go func() { defer GinkgoRecover(); Expect(up1.Start(ctx1)).To(Succeed()) }()
		go func() { defer GinkgoRecover(); Expect(u1.Start(ctx1)).To(Succeed()) }()
		addr := up1.Address()

		add := zset.New()
		add.Insert(cluster("c1", "STATIC"), 1)
		Expect(pub.Publish(dbspruntime.Event{Name: "up1-src", Data: add})).To(Succeed())

		w, err := NewWatcher(WatcherConfig{
			Name: "restart-watcher", InputName: "ingested-r", Type: "cds",
			Address: addr, Runtime: rt,
		})
		Expect(err).NotTo(HaveOccurred())
		coll := newCollector(ctx, rt, "ingested-r")
		go func() { defer GinkgoRecover(); Expect(w.Start(ctx)).To(Succeed()) }()

		Eventually(coll.liveNames, 3*time.Second).Should(ConsistOf("c1"))

		// Kill the upstream and bring a NEW one up on the same address,
		// serving c2 only: c1 vanished during the outage, so nothing ever
		// retracts it explicitly: the resync must.
		cancel1()
		var up2 *Server
		Eventually(func() error {
			s, err := NewServer(ServerConfig{Address: addr, Runtime: rt})
			if err == nil {
				up2 = s
			}
			return err
		}, 5*time.Second).Should(Succeed())
		u2, err := up2.Updater(UpdaterConfig{Name: "up2-cds", OutputName: "up2-src", Type: "cds"})
		Expect(err).NotTo(HaveOccurred())
		go func() { defer GinkgoRecover(); Expect(up2.Start(ctx)).To(Succeed()) }()
		go func() { defer GinkgoRecover(); Expect(u2.Start(ctx)).To(Succeed()) }()
		add2 := zset.New()
		add2.Insert(cluster("c2", "EDS"), 1)
		Expect(pub.Publish(dbspruntime.Event{Name: "up2-src", Data: add2})).To(Succeed())

		Eventually(coll.liveNames, 10*time.Second).Should(ConsistOf("c2"))
	})
})
