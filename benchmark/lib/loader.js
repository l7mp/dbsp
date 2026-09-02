// ShardedLoader: parallel bulk writes through the kubernetes connector.
//
// Every update consumer is one Go goroutine applying its topic's entries
// sequentially, so a single consumer loads at one-API-call-at-a-time pace.
// The loader spreads one GVK over K consumers and routes each document by a
// hash of its object key: distinct objects load in parallel, while writes
// to the SAME object always land on the same shard, preserving the
// per-object FIFO that correctness needs. Cross-object ordering is not
// preserved - callers sequencing dependent kinds (a namespace before its
// pods) wait for the dependency to land first.
//
// The other half of load throughput is the client-side rate limit: all
// consumers share the runtime's one client, so start the runtime with a
// driver-sized budget (kubernetes.runtime.start({qps, burst})) or the
// shards just take turns waiting on the limiter.

class ShardedLoader {
  // opts: { gvk, prefix, shards }. Topics are `${prefix}.${i}`.
  constructor({ gvk, prefix, shards = 8 }) {
    this.topics = [];
    for (let s = 0; s < shards; s++) {
      const topic = `${prefix}.${s}`;
      kubernetes.update(topic, { gvk });
      this.topics.push(topic);
    }
  }

  // write publishes [doc, weight] entries, routed per document.
  write(docs, weight = 1) {
    const batches = this.topics.map(() => []);
    for (const doc of docs) {
      batches[this.shard(doc)].push([doc, weight]);
    }
    for (let s = 0; s < batches.length; s++) {
      if (batches[s].length > 0) {
        publish(this.topics[s], batches[s]);
      }
    }
  }

  // shard hashes the object key (FNV-1a over namespace/name).
  shard(doc) {
    const key = `${doc.metadata?.namespace || ""}/${doc.metadata?.name || ""}`;
    let h = 2166136261;
    for (let i = 0; i < key.length; i++) {
      h ^= key.charCodeAt(i);
      h = Math.imul(h, 16777619);
    }
    return (h >>> 0) % this.topics.length;
  }
}

module.exports = { ShardedLoader };
