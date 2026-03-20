package main

import (
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("SQL commands", func() {
	var (
		state *appState
		run   func(...string) error
	)

	BeforeEach(func() {
		state, run = newTestEnv()
	})

	Describe("create", func() {
		It("creates a named SQL statement", func() {
			Expect(run("sql", "create", "q1", "SELECT", "*", "FROM", "t")).To(Succeed())
			Expect(state.sql).To(HaveKey("q1"))
		})

		It("accepts explicit output name", func() {
			Expect(run("sql", "create", "--output", "my-out", "q1", "SELECT", "*", "FROM", "t")).To(Succeed())
			Expect(state.sql["q1"].Output).To(Equal("my-out"))
		})

		It("errors on invalid syntax", func() {
			Expect(run("sql", "create", "bad", "SELECT", "FROM")).
				To(MatchError(ContainSubstring("parse")))
		})
	})

	Describe("compile", func() {
		BeforeEach(func() {
			Expect(run("sql", "table", "create", "TABLE", "t", "(id", "INT,", "score", "INT)")).To(Succeed())
			Expect(run("sql", "create", "q1", "SELECT", "*", "FROM", "t")).To(Succeed())
		})

		It("compiles named SQL into a named circuit", func() {
			Expect(run("sql", "compile", "q1", "c1")).To(Succeed())
			Expect(state.circuits).To(HaveKey("c1"))
		})

		It("errors when statement does not exist", func() {
			Expect(run("sql", "compile", "missing", "c1")).To(MatchError(ContainSubstring("not found")))
		})

		It("errors when circuit name already exists", func() {
			Expect(run("sql", "compile", "q1", "c1")).To(Succeed())
			Expect(run("sql", "compile", "q1", "c1")).To(MatchError(ContainSubstring("already exists")))
		})

		It("uses default output name <sql-name>-output", func() {
			Expect(run("sql", "compile", "q1", "c1")).To(Succeed())
			Expect(state.queries["c1"].OutputMap).To(HaveKey("q1-output"))
		})

		It("uses explicit output name set by sql output", func() {
			Expect(run("sql", "create", "--output", "my-out", "q2", "SELECT", "*", "FROM", "t")).To(Succeed())
			Expect(run("sql", "compile", "q2", "c1")).To(Succeed())
			Expect(state.queries["c1"].OutputMap).To(HaveKey("my-out"))
		})
	})

	Describe("insert", func() {
		BeforeEach(func() {
			Expect(run("sql", "table", "create", "TABLE", "t", "(id", "INT,", "score", "INT)")).To(Succeed())
		})

		It("inserts a row into the table", func() {
			Expect(run("sql", "insert", "INTO", "t", "VALUES", "(1,", "100)")).To(Succeed())
			table, err := state.db.GetTable("t")
			Expect(err).NotTo(HaveOccurred())
			Expect(table.ToZSet().Size()).To(Equal(1))
		})

		It("inserts multiple rows", func() {
			Expect(run("sql", "insert", "INTO", "t", "VALUES", "(1,", "100)")).To(Succeed())
			Expect(run("sql", "insert", "INTO", "t", "VALUES", "(2,", "200)")).To(Succeed())
			table, _ := state.db.GetTable("t")
			Expect(table.ToZSet().Size()).To(Equal(2))
		})

		It("errors on unknown table", func() {
			Expect(run("sql", "insert", "INTO", "unknown", "VALUES", "(1,", "2)")).
				To(MatchError(ContainSubstring("not found")))
		})
	})

	Describe("select", func() {
		BeforeEach(func() {
			Expect(run("sql", "table", "create", "TABLE", "t", "(id", "INT,", "score", "INT)")).To(Succeed())
			Expect(run("sql", "insert", "INTO", "t", "VALUES", "(1,", "42)")).To(Succeed())
		})

		It("executes a SELECT * without error", func() {
			Expect(run("sql", "select", "*", "from", "t")).To(Succeed())
		})

		It("saves the compiled circuit with --save", func() {
			Expect(run("sql", "select", "--save", "mycirc", "*", "from", "t")).To(Succeed())
			Expect(state.circuits).To(HaveKey("mycirc"))
		})

		It("errors on --save with an existing circuit name", func() {
			Expect(run("sql", "select", "--save", "mycirc", "*", "from", "t")).To(Succeed())
			Expect(run("sql", "select", "--save", "mycirc", "*", "from", "t")).
				To(MatchError(ContainSubstring("already exists")))
		})

		It("errors on unknown table reference", func() {
			Expect(run("sql", "select", "*", "from", "nope")).To(HaveOccurred())
		})
	})

	Describe("drop", func() {
		It("drops a table from the database", func() {
			Expect(run("sql", "table", "create", "TABLE", "t", "(id", "INT)")).To(Succeed())
			Expect(run("sql", "table", "drop", "TABLE", "t")).To(Succeed())
			_, err := state.db.GetTable("t")
			Expect(err).To(HaveOccurred())
		})

		It("errors on unknown table", func() {
			Expect(run("sql", "table", "drop", "TABLE", "nope")).
				To(MatchError(ContainSubstring("not found")))
		})
	})

	Describe("tables", func() {
		It("lists all tables without error", func() {
			Expect(run("sql", "table", "create", "TABLE", "t1", "(id", "INT)")).To(Succeed())
			Expect(run("sql", "table", "create", "TABLE", "t2", "(id", "INT)")).To(Succeed())
			Expect(run("sql", "tables")).To(Succeed())
			Expect(state.db.Tables()).To(ConsistOf("t1", "t2"))
		})
	})

	Describe("schema", func() {
		It("shows schema without error", func() {
			Expect(run("sql", "table", "create", "TABLE", "t", "(id", "INT,", "name", "TEXT)")).To(Succeed())
			Expect(run("sql", "schema", "t")).To(Succeed())
		})

		It("errors on unknown table", func() {
			Expect(run("sql", "schema", "nope")).To(HaveOccurred())
		})
	})

	Describe("eval", func() {
		BeforeEach(func() {
			Expect(run("sql", "table", "create", "TABLE", "t", "(id", "INT,", "score", "INT)")).To(Succeed())
			Expect(run("sql", "insert", "INTO", "t", "VALUES", "(1,", "42)")).To(Succeed())
			Expect(run("sql", "insert", "INTO", "t", "VALUES", "(2,", "7)")).To(Succeed())
		})

		It("evaluates a full SELECT query", func() {
			Expect(run("sql", "eval", "SELECT", "*", "FROM", "t")).To(Succeed())
		})

		It("saves output into a Z-set with --save-zset", func() {
			Expect(run("sql", "eval", "--save-zset", "res", "SELECT", "*", "FROM", "t")).To(Succeed())
			Expect(state.zsets).To(HaveKey("res"))
			Expect(state.zsets["res"].data.Size()).To(Equal(2))
		})

		It("saves the circuit with --save", func() {
			Expect(run("sql", "eval", "--save", "mycirc", "SELECT", "*", "FROM", "t")).To(Succeed())
			Expect(state.circuits).To(HaveKey("mycirc"))
		})

		It("errors on duplicate --save name", func() {
			Expect(run("sql", "eval", "--save", "c", "SELECT", "*", "FROM", "t")).To(Succeed())
			Expect(run("sql", "eval", "--save", "c", "SELECT", "*", "FROM", "t")).
				To(MatchError(ContainSubstring("already exists")))
		})

		It("errors on duplicate --save-zset name", func() {
			Expect(run("sql", "eval", "--save-zset", "z", "SELECT", "*", "FROM", "t")).To(Succeed())
			Expect(run("sql", "eval", "--save-zset", "z", "SELECT", "*", "FROM", "t")).
				To(MatchError(ContainSubstring("already exists")))
		})

		It("produces the same result with --incr as without for a single table", func() {
			Expect(run("sql", "eval", "--incr", "--save-zset", "ir", "SELECT", "*", "FROM", "t")).To(Succeed())
			Expect(state.zsets).To(HaveKey("ir"))
			Expect(state.zsets["ir"].data.Size()).To(Equal(2))
		})

		It("evaluates a JOIN with --incr", func() {
			Expect(run("sql", "table", "create", "TABLE", "products",
				"(pid", "INT", "PRIMARY", "KEY,", "name", "TEXT)")).To(Succeed())
			Expect(run("sql", "table", "create", "TABLE", "orders",
				"(oid", "INT", "PRIMARY", "KEY,", "product_id", "INT,", "qty", "INT)")).To(Succeed())
			Expect(run("sql", "insert", "INTO", "products", "VALUES", "(1,", "'Widget')")).To(Succeed())
			Expect(run("sql", "insert", "INTO", "orders", "VALUES", "(101,", "1,", "3)")).To(Succeed())

			Expect(run("sql", "eval", "--incr", "--save-zset", "jr",
				"SELECT", "oid,", "pid,", "name,", "qty",
				"FROM", "orders", "JOIN", "products", "ON", "product_id", "=", "pid")).To(Succeed())
			Expect(state.zsets).To(HaveKey("jr"))
			Expect(state.zsets["jr"].data.Size()).To(Equal(1))
		})
	})
})
