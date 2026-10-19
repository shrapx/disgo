package gateway

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// With SingleAttempt a gateway dials once per Open, and whatever ends the connection (a close frame,
// a dropped connection, opcode 7, opcode 9, a failed heartbeat send) calls the close handler once and
// opens nothing: the caller decides whether to connect again. These tests count the server's side:
// dials (every HTTP request to the gateway URL, refused or not) and Identify/Resume commands.

// closes records the calls of the close handler
type closes struct {
	mu   sync.Mutex
	errs []error
}

func (c *closes) handler(_ Gateway, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs = append(c.errs, err)
}

func (c *closes) get() []error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]error(nil), c.errs...)
}

// settle is how long a reconnect that is going to happen is given to happen: the deployed fork's
// reconnect of opcode 7 and 9 and of a failed heartbeat waits 0 s before its first dial
const settle = 1500 * time.Millisecond

// expectOneAttempt asserts, after settle, that the server saw exactly one dial, one Identify, no
// Resume, and that the close handler ran exactly once; it returns the error the handler got.
func expectOneAttempt(t *testing.T, f *fakeGateway, c *closes) error {
	t.Helper()
	time.Sleep(settle)
	attempts, accepted, identifies, resumes := f.counts()
	assert.Equal(t, 1, attempts, "dials")
	assert.Equal(t, 1, accepted, "accepted connections")
	assert.Equal(t, 1, identifies, "Identify commands")
	assert.Equal(t, 0, resumes, "Resume commands")
	errs := c.get()
	if !assert.Len(t, errs, 1, "close handler calls") {
		return nil
	}
	return errs[0]
}

func newSingle(t *testing.T, heartbeatMs int, autoReconnect bool) (*fakeGateway, Gateway, *closes) {
	t.Helper()
	f := newFakeGateway(t, heartbeatMs, false)
	f.singleAttempt = true
	c := &closes{}
	g, _ := f.open(t, autoReconnect, c.handler)
	return f, g, c
}

func TestSingleAttemptOpcodeReconnect(t *testing.T) {
	f, g, c := newSingle(t, 45000, false)

	f.sendOp(OpcodeReconnect, nil)
	err := expectOneAttempt(t, f, c)

	require.ErrorIs(t, err, ErrReconnectRequested)
	require.Equal(t, StatusDisconnected, g.Status())
	require.NotNil(t, g.SessionID(), "opcode 7 keeps the session, so the next client can resume")
}

func TestSingleAttemptOpcodeInvalidSession(t *testing.T) {
	for _, tc := range []struct {
		name       string
		canResume  bool
		wantErr    error
		wantResume bool
	}{
		{"d true", true, ErrInvalidSessionResumable, true},
		{"d false", false, ErrInvalidSession, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, g, c := newSingle(t, 45000, false)

			f.sendOp(OpcodeInvalidSession, tc.canResume)
			err := expectOneAttempt(t, f, c)

			require.ErrorIs(t, err, tc.wantErr)
			require.Equal(t, StatusDisconnected, g.Status())
			require.Equal(t, tc.wantResume, g.SessionID() != nil, "session kept")
		})
	}
}

// the server does not answer heartbeats: the gateway has no ack check, so it neither reconnects nor
// calls the close handler (the manager's own 5-minute health check is what ends a silent connection)
func TestSingleAttemptNoHeartbeatAcks(t *testing.T) {
	f := newFakeGateway(t, 20, false)
	f.noAck = true
	f.singleAttempt = true
	c := &closes{}
	g, _ := f.open(t, false, c.handler)

	eventually(t, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.heartbeats >= 10
	}, "heartbeats sent")
	time.Sleep(settle)

	attempts, accepted, identifies, resumes := f.counts()
	assert.Equal(t, 1, attempts, "dials")
	assert.Equal(t, 1, accepted, "accepted connections")
	assert.Equal(t, 1, identifies, "Identify commands")
	assert.Equal(t, 0, resumes, "Resume commands")
	assert.Empty(t, c.get(), "close handler calls")
	require.NotEqual(t, StatusDisconnected, g.Status())
}

func TestSingleAttemptClose(t *testing.T) {
	t.Run("1001 close frame", func(t *testing.T) {
		f, _, c := newSingle(t, 45000, false)
		f.closeCurrent(websocket.CloseGoingAway, "going away")
		err := expectOneAttempt(t, f, c)
		var ce *websocket.CloseError
		require.ErrorAs(t, err, &ce)
		require.Equal(t, websocket.CloseGoingAway, ce.Code)
	})
	t.Run("1006 dropped connection", func(t *testing.T) {
		f, _, c := newSingle(t, 45000, false)
		f.dropCurrent()
		err := expectOneAttempt(t, f, c)
		var ce *websocket.CloseError
		require.ErrorAs(t, err, &ce)
		require.Equal(t, websocket.CloseAbnormalClosure, ce.Code)
	})
	// SingleAttempt wins over AutoReconnect: nothing reconnects
	t.Run("1001 with AutoReconnect on", func(t *testing.T) {
		f, _, c := newSingle(t, 45000, true)
		f.closeCurrent(websocket.CloseGoingAway, "going away")
		err := expectOneAttempt(t, f, c)
		var ce *websocket.CloseError
		require.ErrorAs(t, err, &ce)
	})
	t.Run("1006 with AutoReconnect on", func(t *testing.T) {
		f, _, c := newSingle(t, 45000, true)
		f.dropCurrent()
		expectOneAttempt(t, f, c)
	})
	// a fatal Discord code: unchanged, the handler gets it
	t.Run("4004", func(t *testing.T) {
		f, _, c := newSingle(t, 45000, false)
		f.closeCurrent(4004, "Authentication failed.")
		err := expectOneAttempt(t, f, c)
		var ce *websocket.CloseError
		require.ErrorAs(t, err, &ce)
		require.Equal(t, 4004, ce.Code)
	})
}

// failingConn is a net.Conn whose writes fail once fail is set; reads go on, so the websocket's
// read side does not see the failure and only the heartbeat's send does
type failingConn struct {
	net.Conn
	fail *atomic.Bool
}

func (c failingConn) Write(b []byte) (int, error) {
	if c.fail.Load() {
		return 0, errors.New("write failed")
	}
	return c.Conn.Write(b)
}

func TestSingleAttemptFailedHeartbeat(t *testing.T) {
	var fail atomic.Bool
	f := newFakeGateway(t, 50, false)
	f.singleAttempt = true
	f.dialer = &websocket.Dialer{
		NetDialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, addr)
			if err != nil {
				return nil, err
			}
			return failingConn{Conn: conn, fail: &fail}, nil
		},
	}
	c := &closes{}
	g, _ := f.open(t, false, c.handler)

	fail.Store(true)
	err := expectOneAttempt(t, f, c)

	require.ErrorIs(t, err, ErrHeartbeatFailed)
	require.Equal(t, StatusDisconnected, g.Status())
}

// a refused dial is the one attempt: Open returns its error at once, and nothing dials again
func TestSingleAttemptOpenDialsOnce(t *testing.T) {
	f := newFakeGateway(t, 45000, false)
	f.singleAttempt = true
	f.setRefuse(true)
	g := f.newGateway(false, nil, make(chan struct{}, 1))

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := g.Open(ctx)
	elapsed := time.Since(start)

	require.Error(t, err)
	assert.Less(t, elapsed, time.Second, "Open returned after its first dial failed")
	f.setRefuse(false)
	time.Sleep(settle + 2*time.Second) // the deployed fork's second try comes 2 s after the first
	attempts, accepted, identifies, _ := f.counts()
	assert.Equal(t, 1, attempts, "dials")
	assert.Equal(t, 0, accepted)
	assert.Equal(t, 0, identifies)
	require.Equal(t, StatusDisconnected, g.Status())

	// the caller's next Open is a new attempt and works
	require.NoError(t, g.Open(ctx))
	closeGateway(g)
}

// the option is off by default: opcode 7 reconnects on the same gateway and resumes, as before
func TestOpcodeReconnectWithoutSingleAttempt(t *testing.T) {
	f := newFakeGateway(t, 45000, false)
	c := &closes{}
	g, ready := f.open(t, false, c.handler)

	f.sendOp(OpcodeReconnect, nil)
	waitReady(t, ready)

	_, accepted, identifies, resumes := f.counts()
	require.Equal(t, 2, accepted)
	require.Equal(t, 1, identifies)
	require.Equal(t, 1, resumes)
	require.Empty(t, c.get())
	require.NotEqual(t, StatusDisconnected, g.Status())
}
