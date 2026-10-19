package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
)

// fakeGateway is a minimal Discord gateway: Hello, Ready for an Identify, Resumed for a Resume,
// heartbeat acks. It can close the current connection, send an opcode and refuse new connections.
type fakeGateway struct {
	t   *testing.T
	srv *httptest.Server
	url string

	heartbeatMs int
	events      bool // send a dispatch every millisecond

	// set before newGateway; the gateway is then built with these
	noAck         bool // do not answer heartbeats
	singleAttempt bool // WithSingleAttempt(true)
	dialer        *websocket.Dialer
	gate          IdentifyGate // WithIdentifyGate

	mu         sync.Mutex
	refuse     bool
	attempts   int
	accepted   int
	identifies int
	resumes    int
	heartbeats int
	current    *fakeConn
}

type fakeConn struct {
	mu   sync.Mutex
	conn *websocket.Conn
}

func (c *fakeConn) write(v any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn.WriteJSON(v)
}

func newFakeGateway(t *testing.T, heartbeatMs int, events bool) *fakeGateway {
	f := &fakeGateway{t: t, heartbeatMs: heartbeatMs, events: events}
	upgrader := websocket.Upgrader{}
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.attempts++
		refuse := f.refuse
		f.mu.Unlock()
		if refuse {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		c := &fakeConn{conn: conn}
		f.mu.Lock()
		f.accepted++
		f.current = c
		f.mu.Unlock()
		go f.serve(c)
	}))
	f.url = "ws" + strings.TrimPrefix(f.srv.URL, "http")
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeGateway) serve(c *fakeConn) {
	defer c.conn.Close()
	_ = c.write(map[string]any{"op": 10, "d": map[string]any{"heartbeat_interval": f.heartbeatMs}})
	done := make(chan struct{})
	defer close(done)
	if f.events {
		go func() {
			ticker := time.NewTicker(time.Millisecond)
			defer ticker.Stop()
			for s := 10; ; s++ {
				select {
				case <-done:
					return
				case <-ticker.C:
					if c.write(map[string]any{"op": 0, "t": "TEST_EVENT", "s": s, "d": map[string]any{}}) != nil {
						return
					}
				}
			}
		}()
	}
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var msg struct {
			Op int             `json:"op"`
			D  json.RawMessage `json:"d"`
		}
		if json.Unmarshal(data, &msg) != nil {
			continue
		}
		switch msg.Op {
		case 1:
			f.mu.Lock()
			f.heartbeats++
			f.mu.Unlock()
			if !f.noAck {
				_ = c.write(map[string]any{"op": 11})
			}
		case 2:
			f.mu.Lock()
			f.identifies++
			f.mu.Unlock()
			_ = c.write(map[string]any{"op": 0, "t": "READY", "s": 1, "d": map[string]any{
				"v":                  10,
				"user":               map[string]any{"id": "1", "username": "bot"},
				"guilds":             []any{},
				"session_id":         "session-1",
				"resume_gateway_url": f.url,
				"application":        map[string]any{"id": "1", "flags": 0},
			}})
		case 6:
			f.mu.Lock()
			f.resumes++
			f.mu.Unlock()
			_ = c.write(map[string]any{"op": 0, "t": "RESUMED", "s": 2, "d": map[string]any{}})
		}
	}
}

// take returns the open connection and forgets it
func (f *fakeGateway) take() *fakeConn {
	f.mu.Lock()
	defer f.mu.Unlock()
	c := f.current
	f.current = nil
	require.NotNil(f.t, c, "no open connection")
	return c
}

// closeCurrent sends a close frame with code to the open connection and ends it
func (f *fakeGateway) closeCurrent(code int, text string) {
	c := f.take()
	c.mu.Lock()
	_ = c.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(code, text))
	c.mu.Unlock()
	_ = c.conn.Close()
}

// dropCurrent ends the open connection without a close frame
func (f *fakeGateway) dropCurrent() {
	_ = f.take().conn.Close()
}

// sendOp sends opcode op with data d on the open connection, which the gateway is then expected to close
func (f *fakeGateway) sendOp(op Opcode, d any) {
	require.NoError(f.t, f.take().write(map[string]any{"op": op, "d": d}))
}

func (f *fakeGateway) setRefuse(v bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.refuse = v
}

func (f *fakeGateway) counts() (attempts, accepted, identifies, resumes int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts, f.accepted, f.identifies, f.resumes
}

func (f *fakeGateway) newGateway(autoReconnect bool, closeHandler CloseHandlerFunc, ready chan<- struct{}) Gateway {
	opts := []ConfigOpt{
		WithURL(f.url),
		WithCompress(false),
		WithEnableResumeURL(true),
		WithAutoReconnect(autoReconnect),
		WithLogger(slog.New(slog.NewTextHandler(io.Discard, nil))),
	}
	if f.singleAttempt {
		opts = append(opts, WithSingleAttempt(true))
	}
	if f.gate != nil {
		opts = append(opts, WithIdentifyGate(f.gate))
	}
	if f.dialer != nil {
		opts = append(opts, WithDialer(f.dialer))
	}
	return New("token",
		func(eventType EventType, _ int, _ int, _ EventData) {
			if eventType == EventTypeReady || eventType == EventTypeResumed {
				select {
				case ready <- struct{}{}:
				default:
				}
			}
		},
		closeHandler,
		opts...,
	)
}

// open opens a gateway to the fake gateway and waits for Ready
func (f *fakeGateway) open(t *testing.T, autoReconnect bool, closeHandler CloseHandlerFunc) (Gateway, chan struct{}) {
	t.Helper()
	ready := make(chan struct{}, 1)
	g := f.newGateway(autoReconnect, closeHandler, ready)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, g.Open(ctx))
	waitReady(t, ready)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		g.Close(ctx)
	})
	return g, ready
}

func waitReady(t *testing.T, ready <-chan struct{}) {
	t.Helper()
	select {
	case <-ready:
	case <-time.After(5 * time.Second):
		t.Fatal("no Ready or Resumed")
	}
}

func eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	require.Eventually(t, cond, 10*time.Second, 10*time.Millisecond, msg)
}

func closeGateway(g Gateway) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	g.Close(ctx)
}

// a reconnectable close with AutoReconnect resumes the session on a new connection
func TestAutoReconnectResumes(t *testing.T) {
	f := newFakeGateway(t, 45000, false)
	g, ready := f.open(t, true, nil)

	f.closeCurrent(websocket.CloseGoingAway, "going away")
	waitReady(t, ready)

	_, accepted, identifies, resumes := f.counts()
	require.Equal(t, 2, accepted)
	require.Equal(t, 1, identifies)
	require.Equal(t, 1, resumes)
	require.Equal(t, "session-1", *g.SessionID())
	// a resumed gateway stays StatusResuming: RESUMED does not set StatusReady (as upstream)
	require.Equal(t, StatusResuming, g.Status())
}

// assertNoReopen: with reconnects refused, Close, then accept again: nothing connects
func assertNoReopenAfterClose(t *testing.T, f *fakeGateway, g Gateway) {
	t.Helper()
	eventually(t, func() bool { attempts, _, _, _ := f.counts(); return attempts >= 2 }, "first reconnect refused")

	closeGateway(g) // what a bot's shutdown does
	f.setRefuse(false)

	// the reconnect's next try would come 4 s after the refused one
	time.Sleep(6 * time.Second)
	attempts, accepted, _, _ := f.counts()
	require.Equal(t, 1, accepted, "the gateway connected again after Close")
	require.LessOrEqual(t, attempts, 3, "the gateway kept trying after Close")
	require.Equal(t, StatusDisconnected, g.Status())
}

// a Close during AutoReconnect's retries ends them: no connection opens after it
func TestCloseStopsAutoReconnect(t *testing.T) {
	f := newFakeGateway(t, 45000, false)
	g, _ := f.open(t, true, nil)

	f.setRefuse(true)
	f.closeCurrent(websocket.CloseGoingAway, "going away")
	assertNoReopenAfterClose(t, f, g)
}

// opcode 7 (Reconnect) reconnects whatever AutoReconnect is; a Close ends that as well
func TestCloseStopsReconnectOfOpcodeReconnect(t *testing.T) {
	f := newFakeGateway(t, 45000, false)
	g, _ := f.open(t, false, nil)

	f.setRefuse(true)
	f.sendOp(OpcodeReconnect, nil)
	assertNoReopenAfterClose(t, f, g)
}

// opcode 9 (Invalid Session) too
func TestCloseStopsReconnectOfOpcodeInvalidSession(t *testing.T) {
	f := newFakeGateway(t, 45000, false)
	g, _ := f.open(t, false, nil)

	f.setRefuse(true)
	f.sendOp(OpcodeInvalidSession, true)
	assertNoReopenAfterClose(t, f, g)
}

// a Close ends Open's own retries too
func TestCloseStopsOpen(t *testing.T) {
	f := newFakeGateway(t, 45000, false)
	f.setRefuse(true)
	g := f.newGateway(false, nil, make(chan struct{}, 1))

	errCh := make(chan error, 1)
	go func() { errCh <- g.Open(context.Background()) }()
	eventually(t, func() bool { attempts, _, _, _ := f.counts(); return attempts >= 2 }, "first tries refused")
	closeGateway(g)
	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("Open did not return after Close")
	}
	f.setRefuse(false)
	time.Sleep(5 * time.Second)
	_, accepted, _, _ := f.counts()
	require.Equal(t, 0, accepted)
}

// Open after Close starts a new run and connects
func TestOpenAfterClose(t *testing.T) {
	f := newFakeGateway(t, 45000, false)
	g, ready := f.open(t, true, nil)

	closeGateway(g)
	require.Equal(t, StatusDisconnected, g.Status())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, g.Open(ctx))
	waitReady(t, ready)
	_, accepted, identifies, _ := f.counts()
	require.Equal(t, 2, accepted)
	require.Equal(t, 2, identifies, "a Close (1000) ends the session, so the new connection identifies")
}

// with AutoReconnect off, a close handler that calls Open (a reconnect of the caller's own) resumes
func TestCloseHandlerReopens(t *testing.T) {
	f := newFakeGateway(t, 45000, false)
	var mu sync.Mutex
	handler := func(gw Gateway, err error) {
		mu.Lock()
		defer mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = gw.Open(ctx)
	}
	g, ready := f.open(t, false, handler)

	f.closeCurrent(websocket.CloseGoingAway, "going away")
	waitReady(t, ready)
	_, accepted, identifies, resumes := f.counts()
	require.Equal(t, 2, accepted)
	require.Equal(t, 1, identifies)
	require.Equal(t, 1, resumes)
	require.Equal(t, StatusResuming, g.Status())
}

// fast heartbeats, a stream of events, server closes, reconnects and Closes while the state is read:
// under -race this covers the fields the reconnect, heartbeat and listen goroutines share
func TestConcurrentCloseReconnectHeartbeat(t *testing.T) {
	f := newFakeGateway(t, 5, true)
	g, ready := f.open(t, true, nil)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = g.Status()
			_ = g.Latency()
			_ = g.SessionID()
			_ = g.LastSequenceReceived()
			time.Sleep(100 * time.Microsecond)
		}
	}()

	for i := 0; i < 5; i++ {
		time.Sleep(30 * time.Millisecond)
		f.closeCurrent(websocket.CloseGoingAway, "going away")
		waitReady(t, ready)
	}
	for i := 0; i < 5; i++ {
		time.Sleep(30 * time.Millisecond)
		closeGateway(g)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		require.NoError(t, g.Open(ctx))
		cancel()
		waitReady(t, ready)
	}
	time.Sleep(30 * time.Millisecond)
	closeGateway(g)
	close(stop)
	wg.Wait()

	f.mu.Lock()
	before := f.heartbeats
	f.mu.Unlock()
	require.Positive(t, before, "no heartbeats were sent")
	time.Sleep(50 * time.Millisecond)
	_, accepted, _, _ := f.counts()
	require.Equal(t, 11, accepted)
	require.Equal(t, StatusDisconnected, g.Status())
}
