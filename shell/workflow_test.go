package main

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("JOIN workflow", func() {
	var (
		state *appState
		run   func(...string) error
	)

	BeforeEach(func() {
		state, run = newTestEnv()
	})

	It("compiles and executes a JOIN, then evaluates incrementally", func() {
		// --- Schema ---
		// All-unique column names so the JOIN ON clause and SELECT list can use
		// unqualified names, making the circuit compatible with both typed
		// relation.Row inputs (sql select) and flat JSON Z-set inputs (executor execute).
		Expect(run("sql", "create", "TABLE", "products",
			"(pid", "INT", "PRIMARY", "KEY,", "name", "TEXT,", "price", "FLOAT)")).To(Succeed())
		Expect(run("sql", "create", "TABLE", "orders",
			"(oid", "INT", "PRIMARY", "KEY,", "product_id", "INT,", "qty", "INT)")).To(Succeed())

		// --- Data ---
		Expect(run("sql", "insert", "INTO", "products", "VALUES", "(1,", "'Widget',", "9.99)")).To(Succeed())
		Expect(run("sql", "insert", "INTO", "products", "VALUES", "(2,", "'Gadget',", "24.99)")).To(Succeed())
		Expect(run("sql", "insert", "INTO", "orders", "VALUES", "(101,", "1,", "3)")).To(Succeed())
		Expect(run("sql", "insert", "INTO", "orders", "VALUES", "(102,", "2,", "1)")).To(Succeed())

		// --- Compile JOIN (SotW) and save circuit ---
		Expect(run("sql", "select", "--save", "join_q",
			"oid,", "product_id,", "pid,", "name,", "price,", "qty",
			"FROM", "orders", "JOIN", "products", "ON", "product_id", "=", "pid")).To(Succeed())

		Expect(state.circuits).To(HaveKey("join_q"))
		Expect(state.circuits["join_q"].Node("input_orders")).NotTo(BeNil())
		Expect(state.circuits["join_q"].Node("input_products")).NotTo(BeNil())

		// --- Build JSON Z-sets mirroring the SQL tables ---
		// Flat JSON with unqualified field names matches what the compiled circuit
		// expects from Unstructured documents after CartesianProduct + Select.
		Expect(run("zset", "create", "products_z")).To(Succeed())
		Expect(run("zset", "insert", "products_z", `({"pid":1,"name":"Widget","price":9.99},1)`)).To(Succeed())
		Expect(run("zset", "insert", "products_z", `({"pid":2,"name":"Gadget","price":24.99},1)`)).To(Succeed())

		Expect(run("zset", "create", "orders_z")).To(Succeed())
		Expect(run("zset", "insert", "orders_z", `({"oid":101,"product_id":1,"qty":3},1)`)).To(Succeed())
		Expect(run("zset", "insert", "orders_z", `({"oid":102,"product_id":2,"qty":1},1)`)).To(Succeed())

		// --- SotW via executor ---
		Expect(run("executor", "create", "sotw_exec", "--circuit", "join_q")).To(Succeed())
		Expect(run("executor", "execute", "sotw_exec",
			"input_orders=orders_z", "input_products=products_z")).To(Succeed())

		Expect(state.zsets).To(HaveKey("sotw_exec-output"))
		Expect(state.zsets["sotw_exec-output"].data.Size()).To(Equal(2))

		// --- Derive incremental twin ---
		// incrementalize creates circuit "join_q-inc" and executor "inc_exec".
		Expect(run("executor", "incrementalize", "sotw_exec", "inc_exec")).To(Succeed())
		Expect(state.circuits).To(HaveKey("join_q-inc"))
		Expect(state.executors).To(HaveKey("inc_exec"))

		// --- Step 1 of incremental executor: seed with full SotW data ---
		// The first delta equals the full dataset; output must match SotW.
		Expect(run("executor", "execute", "inc_exec",
			"input_orders=orders_z", "input_products=products_z")).To(Succeed())

		Expect(state.zsets).To(HaveKey("inc_exec-output"))
		Expect(state.zsets["inc_exec-output"].data.Size()).To(Equal(2))

		// --- Step 2: delta — one new order, empty products delta ---
		Expect(run("zset", "create", "delta_orders")).To(Succeed())
		Expect(run("zset", "insert", "delta_orders", `({"oid":103,"product_id":1,"qty":7},1)`)).To(Succeed())
		Expect(run("zset", "create", "empty_z")).To(Succeed())

		Expect(run("executor", "execute", "inc_exec",
			"input_orders=delta_orders", "input_products=empty_z")).To(Succeed())

		// Only the one new join tuple should appear in the incremental output.
		Expect(state.zsets["inc_exec-output"].data.Size()).To(Equal(1))
	})
})
