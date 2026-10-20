//go:build !tinygo

package agent

import (
	"io"
	"net"
	"testing"
	"time"

	"github.com/chainreactors/rem/protocol/core"
	"github.com/chainreactors/rem/protocol/message"
	"github.com/chainreactors/rem/x/yamux"
)

// newRedirectTestPair builds one client agent and its server twin over a
// net.Pipe with real yamux, the same wiring the console performs, and starts
// the stream accept loops on both. Only the server twin goes into the Agents
// registry, mirroring the listener process.
func newRedirectTestPair(t *testing.T, id string) (client, twin *Agent) {
	t.Helper()

	clientRaw, serverRaw := net.Pipe()
	client = newBridgeTestAgentBase(t, id, core.CLIENT, clientRaw)
	twin = newBridgeTestAgentBase(t, id, core.SERVER, serverRaw)

	cfg := yamux.DefaultConfig()
	cfg.LogOutput = io.Discard
	cfg.EnableKeepAlive = false
	cfg.ConnectionWriteTimeout = 5 * time.Second
	cfg.StreamOpenTimeout = 5 * time.Second

	var err error
	client.session, err = yamux.Client(clientRaw, cfg)
	if err != nil {
		t.Fatalf("yamux client: %v", err)
	}
	twin.session, err = yamux.Server(serverRaw, cfg)
	if err != nil {
		t.Fatalf("yamux server: %v", err)
	}

	client.sessionConnID = id + "-client"
	twin.sessionConnID = id + "-server"

	if _, err = client.connHub.AddClientConn(client.sessionConnID, id+"-client", clientRaw, client.session); err != nil {
		t.Fatalf("add client conn: %v", err)
	}
	if _, err = twin.connHub.AddServerConn(twin.sessionConnID, id+"-server", serverRaw, twin.session); err != nil {
		t.Fatalf("add server conn: %v", err)
	}

	startBridgeAcceptLoop(client)
	startBridgeAcceptLoop(twin)
	return client, twin
}

// TestRedirectBridge_DataPlaneCrossesServer reproduces the -d redirect
// blackhole: a BridgeOpen addressed to another agent (Destination != the
// receiving server twin's id) used to terminate on the twin, which runs no
// inbound/outbound, so the data stream attached to a bridge that never
// joined and the connection blackholed. The open, the data stream, and the
// close must all cross the intermediate twin to the destination pair.
func TestRedirectBridge_DataPlaneCrossesServer(t *testing.T) {
	resetAgentsState()
	t.Cleanup(resetAgentsState)

	// opClient is the -d destination (operator console in production), impClient
	// the redirect agent that accepted the forwarded control (implant side).
	opClient, opTwin := newRedirectTestPair(t, "op")
	impClient, impTwin := newRedirectTestPair(t, "imp")
	Agents.Add(opTwin)
	Agents.Add(impTwin)

	// The twins run the real message dispatch (redirect forwarding included).
	twinDone := make(chan struct{}, 2)
	go func() { impTwin.handleMessage(); twinDone <- struct{}{} }()
	go func() { opTwin.handleMessage(); twinDone <- struct{}{} }()
	// The clients run the plain test control loop: client agents never forward.
	clientCtrlDone := startBridgeControlLoop(opClient)
	impCtrlDone := startBridgeControlLoop(impClient)

	defer func() {
		opClient.Close(nil)
		impClient.Close(nil)
		opTwin.Close(nil)
		impTwin.Close(nil)
		<-clientCtrlDone
		<-impCtrlDone
		<-twinDone
		<-twinDone
	}()

	// The destination side answers the bridge with an echo outbound.
	echoApp, echoBridge := net.Pipe()
	defer echoApp.Close()
	opClient.Outbound = &bridgeTestOutbound{conn: echoBridge, ready: make(chan struct{})}
	go func() {
		io.Copy(echoApp, echoApp) // echo server on the far end of the outbound
	}()

	// The implant side accepts a TCP connection and bridges it toward the
	// operator, exactly as handlerInbound's Serve loop does with the
	// forwarded control (Source=op, Destination=imp).
	appConn, bridgeSide := net.Pipe()
	bridge, err := NewBridgeWithConn(impClient, bridgeSide, &message.Control{Source: "op", Destination: "imp"})
	if err != nil {
		t.Fatalf("NewBridgeWithConn: %v", err)
	}
	impClient.saveBridge(bridge)

	// Routes must be recorded on both twins for stream and close relaying.
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, fwd := impTwin.redirectRoutes.Load(bridge.id)
		_, rev := opTwin.redirectRoutes.Load(bridge.id)
		if fwd && rev {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, ok := impTwin.redirectRoutes.Load(bridge.id); !ok {
		t.Fatal("impTwin did not record the redirect route")
	}
	if _, ok := opTwin.redirectRoutes.Load(bridge.id); !ok {
		t.Fatal("opTwin did not record the reverse redirect route")
	}

	// Data must cross both twins end to end.
	payload := []byte("redirect-echo")
	appConn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := appConn.Write(payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	buf := make([]byte, len(payload))
	if _, err := io.ReadFull(appConn, buf); err != nil {
		t.Fatalf("read echo: %v", err)
	}
	if string(buf) != string(payload) {
		t.Fatalf("payload mismatch: %q", buf)
	}

	// Closing the accepted conn closes the destination bridge through the
	// forwarded BridgeClose. The test control loop deletes closed bridges
	// from the map, so "closed or gone" is the success signal. The delivered
	// close must also retire the redirect routes on both twins — a leftover
	// entry would leak per bridge and could mis-relay a later bridge whose
	// id collides after a console restart.
	appConn.Close()
	deadline = time.Now().Add(3 * time.Second)
	closed := false
	for time.Now().Before(deadline) {
		desBridge, err := opClient.getBridge(bridge.id)
		if err != nil || desBridge.closed.Load() {
			closed = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !closed {
		t.Fatal("destination bridge was not closed through the redirect route")
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, fwd := impTwin.redirectRoutes.Load(bridge.id)
		_, rev := opTwin.redirectRoutes.Load(bridge.id)
		if !fwd && !rev {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("redirect routes were not retired after the close crossed the hop")
}
