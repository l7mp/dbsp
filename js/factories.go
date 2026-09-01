package js

import (
	k8sconn "github.com/l7mp/dbsp/connectors/kubernetes"
	misc "github.com/l7mp/dbsp/connectors/misc"
	xds "github.com/l7mp/dbsp/connectors/xds"
	dbspruntime "github.com/l7mp/dbsp/engine/runtime"
)

// registerConnectorFactories builds the VM's built-in connector
// factories, their environments closed over this VM (the host-owned k8s
// runtime and the script-started xDS servers). Every created runtime
// gets its own connector registry populated from them.
func (v *VM) registerConnectorFactories() error {
	v.factories = []dbspruntime.ConnectorFactory{
		misc.NewFactory(),
		xds.NewFactory(xds.Env{Server: v.lookupXDSServer}),
		k8sconn.NewFactory(k8sconn.Env{Runtime: v.ensureK8sRuntime}),
	}
	return nil
}

// lookupXDSServer resolves a script-started (host-owned) egress server.
func (v *VM) lookupXDSServer(name string) (*xds.Server, bool) {
	v.xdsMu.Lock()
	defer v.xdsMu.Unlock()
	srv, ok := v.xdsServers[name]
	return srv, ok
}
