# EndpointSlice Hybrid Consumer

This example shows the hybrid style supported by Δ-controller. The declarative part watches
`Service` and `EndpointSlice` resources and produces an `EndpointView`, while the Go example
program consumes the resulting view deltas directly.

The maintained tutorial is in [`doc/apps-dctl-tutorial-endpointslice.md`](/doc/apps-dctl-tutorial-endpointslice.md).
