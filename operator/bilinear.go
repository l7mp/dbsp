package operator

import "github.com/l7mp/dbsp/zset"

// Pair is the result of Cartesian product.
type Pair struct {
	left  zset.Element
	right zset.Element
	key   string
}

// NewPair creates a new Pair from two elements.
func NewPair(left, right zset.Element) *Pair {
	return &Pair{
		left:  left,
		right: right,
		key:   "(" + left.Key() + "," + right.Key() + ")",
	}
}

// Key implements zset.Element.
func (p *Pair) Key() string { return p.key }

// Left returns the left element.
func (p *Pair) Left() zset.Element { return p.left }

// Right returns the right element.
func (p *Pair) Right() zset.Element { return p.right }

// CartesianProduct computes A x B.
type CartesianProduct struct {
	name string
}

// NewCartesianProduct creates a new CartesianProduct operator.
func NewCartesianProduct(name string) *CartesianProduct {
	return &CartesianProduct{name: name}
}

// Name implements Operator.
func (o *CartesianProduct) Name() string { return o.name }

// Arity implements Operator.
func (o *CartesianProduct) Arity() int { return 2 }

// Linearity implements Operator.
func (o *CartesianProduct) Linearity() Linearity { return Bilinear }

// Apply implements Operator.
func (o *CartesianProduct) Apply(inputs ...zset.ZSet) (zset.ZSet, error) {
	left, right := inputs[0], inputs[1]
	result := zset.New()

	left.Iter(func(l zset.Element, lw zset.Weight) bool {
		right.Iter(func(r zset.Element, rw zset.Weight) bool {
			result.Insert(NewPair(l, r), lw*rw)
			return true
		})
		return true
	})

	return result, nil
}
