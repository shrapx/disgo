package gateway

import (
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// windowGate is a sliding window of limit Identifies per window, the shape of the manager's gate
// (infra-chat discord.IdentifyBudget), with a clock the test moves.
type windowGate struct {
	mu     sync.Mutex
	limit  int
	window time.Duration
	now    func() time.Time
	times  []time.Time
	taken  int // TakeIdentify calls that returned ok
	held   int // calls that returned not ok
}

func (w *windowGate) TakeIdentify() (bool, time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()
	now := w.now()
	for len(w.times) > 0 && now.Sub(w.times[0]) >= w.window {
		w.times = w.times[1:]
	}
	if len(w.times) >= w.limit {
		w.held++
		return false, w.window - now.Sub(w.times[0])
	}
	w.times = append(w.times, now)
	w.taken++
	return true, 0
}

func (w *windowGate) counts() (taken, held int) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.taken, w.held
}

func newWindowGate(limit int, window time.Duration) *windowGate {
	return &windowGate{limit: limit, window: window, now: time.Now}
}

// without a limit hit the gate is asked once per Identify, and a Resume is never asked
func TestIdentifyGateCountsIdentifiesNotResumes(t *testing.T) {
	f := newFakeGateway(t, 45000, false)
	gate := newWindowGate(10, time.Hour)
	f.gate = gate
	_, ready := f.open(t, true, nil)

	f.closeCurrent(websocket.CloseGoingAway, "going away") // resumable close: the gateway resumes
	waitReady(t, ready)

	_, accepted, identifies, resumes := f.counts()
	require.Equal(t, 2, accepted)
	require.Equal(t, 1, identifies)
	require.Equal(t, 1, resumes)
	taken, held := gate.counts()
	require.Equal(t, 1, taken, "the first Identify is counted, the Resume is not")
	require.Zero(t, held)
}

// opcode 7 and opcode 9 reconnects of a gateway without WithSingleAttempt send an Identify (opcode 9 with d
// false) and are counted, whoever starts them
func TestIdentifyGateCountsOpcodeReconnects(t *testing.T) {
	f := newFakeGateway(t, 45000, false)
	gate := newWindowGate(10, time.Hour)
	f.gate = gate
	_, ready := f.open(t, false, nil)

	// opcode 7: reconnects and resumes, so it is not counted
	f.sendOp(OpcodeReconnect, nil)
	waitReady(t, ready)
	_, _, identifies, resumes := f.counts()
	require.Equal(t, 1, identifies)
	require.Equal(t, 1, resumes)
	taken, _ := gate.counts()
	require.Equal(t, 1, taken, "opcode 7 resumes: not counted")

	// opcode 9, d false: the session is dropped and the reconnect identifies again
	f.sendOp(OpcodeInvalidSession, false)
	waitReady(t, ready)
	_, _, identifies, _ = f.counts()
	require.Equal(t, 2, identifies)
	taken, _ = gate.counts()
	require.Equal(t, 2, taken, "opcode 9 d false re-identifies: counted")

	// opcode 9, d false again
	f.sendOp(OpcodeInvalidSession, false)
	waitReady(t, ready)
	_, _, identifies, _ = f.counts()
	require.Equal(t, 3, identifies)
	taken, held := gate.counts()
	require.Equal(t, 3, taken)
	require.Zero(t, held)
}

// at the limit the Identify is not sent, nothing opens a second connection for it, and it goes once the oldest
// Identify has aged out
func TestIdentifyGateHoldsUntilOldestAgesOut(t *testing.T) {
	f := newFakeGateway(t, 45000, false)
	gate := newWindowGate(1, 1500*time.Millisecond)
	f.gate = gate
	_, ready := f.open(t, false, nil)
	opened := time.Now()

	// d false drops the session; the second Identify is over the limit (1 per 1.5 s) and is held
	f.sendOp(OpcodeInvalidSession, false)
	eventually(t, func() bool { _, held := gate.counts(); return held > 0 }, "no hold")
	_, _, identifies, _ := f.counts()
	require.Equal(t, 1, identifies, "held: no second Identify yet")

	waitReady(t, ready)
	require.GreaterOrEqual(t, time.Since(opened), 1500*time.Millisecond, "sent only after the first aged out")
	_, _, identifies, _ = f.counts()
	require.Equal(t, 2, identifies)
}

// a Close during a hold ends it and sends nothing
func TestIdentifyGateCloseEndsHold(t *testing.T) {
	f := newFakeGateway(t, 45000, false)
	gate := newWindowGate(1, time.Hour)
	f.gate = gate
	g, _ := f.open(t, false, nil)

	f.sendOp(OpcodeInvalidSession, false)
	eventually(t, func() bool { _, held := gate.counts(); return held > 0 }, "no hold")
	closeGateway(g)

	time.Sleep(100 * time.Millisecond)
	_, _, identifies, _ := f.counts()
	require.Equal(t, 1, identifies)
	taken, _ := gate.counts()
	require.Equal(t, 1, taken)
}
