package gateway

import (
	"bytes"
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/disgoorg/json"
	"github.com/gorilla/websocket"

	"github.com/disgoorg/disgo/discord"
)

var _ Gateway = (*gatewayImpl)(nil)

// New creates a new Gateway instance with the provided token, eventHandlerFunc, closeHandlerFunc and ConfigOpt(s).
func New(token string, eventHandlerFunc EventHandlerFunc, closeHandlerFunc CloseHandlerFunc, opts ...ConfigOpt) Gateway {
	config := DefaultConfig()
	config.Apply(opts)
	config.Logger = config.Logger.With(slog.String("name", "gateway"), slog.Int("shard_id", config.ShardID), slog.Int("shard_count", config.ShardCount))

	// closeHandlerFunc prefer config over parameter
	if config.CloseHandlerFunc != nil {
		closeHandlerFunc = config.CloseHandlerFunc
	}

	return &gatewayImpl{
		config:           *config,
		eventHandlerFunc: eventHandlerFunc,
		closeHandlerFunc: closeHandlerFunc,
		token:            token,
		status:           StatusUnconnected,
	}
}

type gatewayImpl struct {
	config           Config
	eventHandlerFunc EventHandlerFunc
	closeHandlerFunc CloseHandlerFunc
	token            string

	// connMu guards conn, heartbeatCancel and status
	conn            *websocket.Conn
	connMu          sync.Mutex
	heartbeatCancel context.CancelFunc
	status          Status

	// runMu guards runCtx and runCancel. A run lasts from Open to the next Close or CloseWithCode
	// of the user; the reconnects the gateway starts by itself run on its context, so a Close ends
	// them and no connection opens after it.
	runMu     sync.Mutex
	runCtx    context.Context
	runCancel context.CancelFunc

	// stateMu guards the session (config.SessionID, config.LastSequenceReceived, config.ResumeURL)
	// and the heartbeat fields. Lock order: connMu before stateMu; runMu is never held with either.
	stateMu               sync.Mutex
	heartbeatInterval     time.Duration
	lastHeartbeatSent     time.Time
	lastHeartbeatReceived time.Time
}

func (g *gatewayImpl) ShardID() int {
	return g.config.ShardID
}

func (g *gatewayImpl) ShardCount() int {
	return g.config.ShardCount
}

func (g *gatewayImpl) SessionID() *string {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	return g.config.SessionID
}

func (g *gatewayImpl) LastSequenceReceived() *int {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	return g.config.LastSequenceReceived
}

func (g *gatewayImpl) Intents() Intents {
	return g.config.Intents
}

func (g *gatewayImpl) Open(ctx context.Context) error {
	run := g.startRun()
	// a Close during Open's retries ends them too
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(run, cancel)
	defer stop()
	if g.config.SingleAttempt {
		return g.openOnce(ctx, run)
	}
	return g.reconnectTry(ctx, run, 0)
}

// openOnce makes the one attempt of a SingleAttempt gateway: no delay, no retry
func (g *gatewayImpl) openOnce(ctx context.Context, run context.Context) error {
	if err := runErr(ctx, run); err != nil {
		return err
	}
	err := g.open(ctx, run)
	if err != nil && !errors.Is(err, discord.ErrGatewayAlreadyConnected) {
		g.setStatus(nil, StatusDisconnected)
	}
	return err
}

// startRun returns the context of the current run, or starts a new run if there is none or the
// last one was ended by a Close
func (g *gatewayImpl) startRun() context.Context {
	g.runMu.Lock()
	defer g.runMu.Unlock()
	if g.runCtx == nil || g.runCtx.Err() != nil {
		g.runCtx, g.runCancel = context.WithCancel(context.Background())
	}
	return g.runCtx
}

// stopRun ends the current run: pending reconnects stop and open nothing
func (g *gatewayImpl) stopRun() {
	g.runMu.Lock()
	defer g.runMu.Unlock()
	if g.runCancel != nil {
		g.runCancel()
	}
}

// open opens a connection for run. ctx bounds the dial; when ctx or run ends before the
// connection is set, nothing is left open.
func (g *gatewayImpl) open(ctx context.Context, run context.Context) error {
	g.config.Logger.Debug("opening gateway connection")

	g.connMu.Lock()
	defer g.connMu.Unlock()
	if g.conn != nil {
		return discord.ErrGatewayAlreadyConnected
	}
	if err := runErr(ctx, run); err != nil {
		return err
	}
	g.status = StatusConnecting

	g.stateMu.Lock()
	wsURL := g.config.URL
	if g.config.ResumeURL != nil && g.config.EnableResumeURL {
		wsURL = *g.config.ResumeURL
	}
	g.lastHeartbeatSent = time.Now().UTC()
	g.stateMu.Unlock()
	gatewayURL := fmt.Sprintf("%s?v=%d&encoding=json", wsURL, Version)
	conn, rs, err := g.config.Dialer.DialContext(ctx, gatewayURL, nil)
	if err != nil {
		body := ""
		if rs != nil && rs.Body != nil {
			defer func() {
				_ = rs.Body.Close()
			}()
			rawBody, bErr := io.ReadAll(rs.Body)
			if bErr != nil {
				g.config.Logger.Error("error while reading response body", slog.Any("err", bErr))
			}
			body = string(rawBody)
		}

		g.config.Logger.Error("error connecting to the gateway", slog.Any("err", err), slog.String("url", gatewayURL), slog.String("body", body))
		return err
	}
	if err = runErr(ctx, run); err != nil {
		// closed while dialing
		_ = conn.Close()
		g.status = StatusDisconnected
		return err
	}

	conn.SetCloseHandler(func(code int, text string) error {
		return nil
	})

	g.conn = conn

	// reset rate limiter when connecting
	g.config.RateLimiter.Reset()

	g.status = StatusWaitingForHello

	go g.listen(conn, run)

	return nil
}

// runErr returns the error of ctx or run, whichever has ended
func runErr(ctx context.Context, run context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return run.Err()
}

func (g *gatewayImpl) Close(ctx context.Context) {
	g.CloseWithCode(ctx, websocket.CloseNormalClosure, "Shutting down")
}

// CloseWithCode closes the connection and ends the run: a reconnect the gateway started by itself
// stops and opens nothing. Open starts a new run.
func (g *gatewayImpl) CloseWithCode(ctx context.Context, code int, message string) {
	g.stopRun()
	g.closeConn(ctx, nil, code, message)
}

// closeConn closes the open connection, or only conn if conn is not nil, and stops its heartbeat;
// the run goes on, so the caller may reconnect. It reports whether it closed a connection.
func (g *gatewayImpl) closeConn(ctx context.Context, conn *websocket.Conn, code int, message string) bool {
	g.connMu.Lock()
	defer g.connMu.Unlock()
	if conn != nil && g.conn != conn {
		// already closed, by the user or another path
		return false
	}
	if g.heartbeatCancel != nil {
		g.config.Logger.Debug("closing heartbeat goroutines...")
		g.heartbeatCancel()
		g.heartbeatCancel = nil
	}

	closed := false
	if g.conn != nil {
		closed = true
		g.config.RateLimiter.Close(ctx)
		g.config.Logger.Debug("closing gateway connection", slog.Int("code", code), slog.String("message", message))
		if err := g.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(code, message)); err != nil && !errors.Is(err, websocket.ErrCloseSent) {
			g.config.Logger.Debug("error writing close code", slog.Any("err", err))
		}
		_ = g.conn.Close()
		g.conn = nil

		// clear resume data as we closed gracefully
		if code == websocket.CloseNormalClosure || code == websocket.CloseGoingAway {
			g.clearSession()
		}
	}
	g.status = StatusDisconnected
	return closed
}

func (g *gatewayImpl) clearSession() {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	g.config.SessionID = nil
	g.config.ResumeURL = nil
	g.config.LastSequenceReceived = nil
}

// setStatus sets the status while conn is still the open connection
func (g *gatewayImpl) setStatus(conn *websocket.Conn, status Status) {
	g.connMu.Lock()
	defer g.connMu.Unlock()
	if g.conn == conn {
		g.status = status
	}
}

func (g *gatewayImpl) Status() Status {
	g.connMu.Lock()
	defer g.connMu.Unlock()
	return g.status
}

func (g *gatewayImpl) Send(ctx context.Context, op Opcode, d MessageData) error {
	data, err := json.Marshal(Message{
		Op: op,
		D:  d,
	})
	if err != nil {
		return err
	}
	return g.send(ctx, websocket.TextMessage, data)
}

func (g *gatewayImpl) send(ctx context.Context, messageType int, data []byte) error {
	g.connMu.Lock()
	defer g.connMu.Unlock()
	if g.conn == nil {
		return discord.ErrShardNotConnected
	}

	if err := g.config.RateLimiter.Wait(ctx); err != nil {
		return err
	}

	defer g.config.RateLimiter.Unlock()
	if g.config.Logger.Enabled(ctx, slog.LevelDebug) {
		g.config.Logger.Debug("sending gateway command", slog.String("data", string(data)))
	}
	return g.conn.WriteMessage(messageType, data)
}

func (g *gatewayImpl) Latency() time.Duration {
	g.stateMu.Lock()
	defer g.stateMu.Unlock()
	return g.lastHeartbeatReceived.Sub(g.lastHeartbeatSent)
}

func (g *gatewayImpl) Presence() *MessageDataPresenceUpdate {
	return g.config.Presence
}

// reconnectTry opens a connection for run, retrying with a growing delay (2 s steps, at most 30 s)
// until it succeeds or ctx or run ends
func (g *gatewayImpl) reconnectTry(ctx context.Context, run context.Context, try int) error {
	for ; ; try++ {
		if err := runErr(ctx, run); err != nil {
			return err
		}
		delay := time.Duration(try) * 2 * time.Second
		if delay > 30*time.Second {
			delay = 30 * time.Second
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-run.Done():
			timer.Stop()
			return run.Err()
		case <-timer.C:
		}

		err := g.open(ctx, run)
		if err == nil {
			return nil
		}
		if errors.Is(err, discord.ErrGatewayAlreadyConnected) {
			return err
		}
		if rErr := runErr(ctx, run); rErr != nil {
			return rErr
		}
		g.config.Logger.Error("failed to reconnect gateway", slog.Any("err", err))
		g.setStatus(nil, StatusDisconnected)
	}
}

// reconnect reopens the gateway for run; a Close of the user ends it
func (g *gatewayImpl) reconnect(run context.Context) {
	err := g.reconnectTry(run, run, 0)
	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, discord.ErrGatewayAlreadyConnected) {
		g.config.Logger.Error("failed to reopen gateway", slog.Any("err", err))
	}
}

// endedByGateway is what follows when the gateway itself ended the connection (opcode 7, opcode 9, a
// failed heartbeat send): it reconnects, whatever AutoReconnect is. With SingleAttempt it opens
// nothing and calls the close handler with err instead.
func (g *gatewayImpl) endedByGateway(run context.Context, err error) {
	if !g.config.SingleAttempt {
		go g.reconnect(run)
		return
	}
	if g.closeHandlerFunc != nil {
		go g.closeHandlerFunc(g, err)
	}
}

// startHeartbeat starts the heartbeat of conn, stopping the one before; nothing if conn is closed
func (g *gatewayImpl) startHeartbeat(conn *websocket.Conn, run context.Context, interval time.Duration) {
	g.connMu.Lock()
	defer g.connMu.Unlock()
	if g.conn != conn {
		return
	}
	if g.heartbeatCancel != nil {
		g.heartbeatCancel()
	}
	ctx, cancel := context.WithCancel(run)
	g.heartbeatCancel = cancel
	go g.heartbeat(ctx, conn, run, interval)
}

func (g *gatewayImpl) heartbeat(ctx context.Context, conn *websocket.Conn, run context.Context, interval time.Duration) {
	heartbeatTicker := time.NewTicker(interval)
	defer heartbeatTicker.Stop()
	defer g.config.Logger.Debug("exiting heartbeat goroutine")

	for {
		select {
		case <-ctx.Done():
			return

		case <-heartbeatTicker.C:
			g.sendHeartbeat(conn, run)
		}
	}
}

func (g *gatewayImpl) sendHeartbeat(conn *websocket.Conn, run context.Context) {
	g.config.Logger.Debug("sending heartbeat")

	g.stateMu.Lock()
	sequence := 0
	if g.config.LastSequenceReceived != nil {
		sequence = *g.config.LastSequenceReceived
	}
	interval := g.heartbeatInterval
	g.stateMu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), interval)
	defer cancel()
	if err := g.Send(ctx, OpcodeHeartbeat, MessageDataHeartbeat(sequence)); err != nil {
		if errors.Is(err, discord.ErrShardNotConnected) || errors.Is(err, syscall.EPIPE) {
			return
		}
		g.config.Logger.Error("failed to send heartbeat", slog.Any("err", err))
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if g.closeConn(closeCtx, conn, websocket.CloseServiceRestart, "heartbeat timeout") {
			g.endedByGateway(run, fmt.Errorf("%w: %w", ErrHeartbeatFailed, err))
		}
		return
	}
	g.stateMu.Lock()
	g.lastHeartbeatSent = time.Now().UTC()
	g.stateMu.Unlock()
}

func (g *gatewayImpl) identify(conn *websocket.Conn) {
	g.setStatus(conn, StatusIdentifying)
	g.config.Logger.Debug("sending Identify command")

	identify := MessageDataIdentify{
		Token: g.token,
		Properties: IdentifyCommandDataProperties{
			OS:      g.config.OS,
			Browser: g.config.Browser,
			Device:  g.config.Device,
		},
		Compress:       g.config.Compress,
		LargeThreshold: g.config.LargeThreshold,
		Intents:        g.config.Intents,
		Presence:       g.config.Presence,
		Shard:          &[2]int{g.ShardID(), g.ShardCount()},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := g.Send(ctx, OpcodeIdentify, identify); err != nil {
		g.config.Logger.Error("error sending Identify command", slog.Any("err", err))
	}
	g.setStatus(conn, StatusWaitingForReady)
}

func (g *gatewayImpl) resume(conn *websocket.Conn, sessionID string, seq int) {
	g.setStatus(conn, StatusResuming)
	resume := MessageDataResume{
		Token:     g.token,
		SessionID: sessionID,
		Seq:       seq,
	}
	g.config.Logger.Debug("sending Resume command")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := g.Send(ctx, OpcodeResume, resume); err != nil {
		g.config.Logger.Error("error sending resume command", slog.Any("err", err))
	}
}

// listen reads conn until it ends; the reconnects it starts belong to run
func (g *gatewayImpl) listen(conn *websocket.Conn, run context.Context) {
	defer g.config.Logger.Debug("exiting listen goroutine")
loop:
	for {
		mt, r, err := conn.NextReader()
		if err != nil {
			g.connMu.Lock()
			sameConnection := g.conn == conn
			g.connMu.Unlock()

			// if sameConnection is false, it means the connection has been closed by the user, and we can just exit
			if !sameConnection {
				return
			}

			reconnect := true
			var closeError *websocket.CloseError
			if errors.As(err, &closeError) {
				closeCode := CloseEventCodeByCode(closeError.Code)
				reconnect = closeCode.Reconnect

				if closeCode == CloseEventCodeInvalidSeq {
					g.clearSession()
				}
				msg := "gateway close received"
				args := []any{
					slog.Bool("reconnect", reconnect),
					slog.Int("code", closeError.Code),
					slog.String("error", closeError.Text),
				}
				if reconnect {
					g.config.Logger.Debug(msg, args...)
				} else {
					g.config.Logger.Error(msg, args...)
				}
			} else if errors.Is(err, net.ErrClosed) {
				// we closed the connection ourselves. Don't try to reconnect here
				reconnect = false
			} else {
				g.config.Logger.Debug("failed to read next message from gateway", slog.Any("err", err))
			}

			// make sure the connection is properly closed
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			closed := g.closeConn(ctx, conn, websocket.CloseServiceRestart, "reconnecting")
			cancel()
			if !closed {
				// the user closed it meanwhile
				return
			}
			if g.config.AutoReconnect && !g.config.SingleAttempt && reconnect {
				go g.reconnect(run)
			} else if g.closeHandlerFunc != nil {
				go g.closeHandlerFunc(g, err)
			}

			break loop
		}

		message, err := g.parseMessage(mt, r)
		if err != nil {
			g.config.Logger.Error("error while parsing gateway message", slog.Any("err", err))
			continue
		}

		switch message.Op {
		case OpcodeHello:
			interval := time.Duration(message.D.(MessageDataHello).HeartbeatInterval) * time.Millisecond
			g.stateMu.Lock()
			g.heartbeatInterval = interval
			g.lastHeartbeatReceived = time.Now().UTC()
			var sessionID *string
			var seq *int
			if g.config.LastSequenceReceived != nil && g.config.SessionID != nil {
				sessionID, seq = g.config.SessionID, g.config.LastSequenceReceived
			}
			g.stateMu.Unlock()
			g.startHeartbeat(conn, run, interval)

			if sessionID == nil {
				g.identify(conn)
			} else {
				g.resume(conn, *sessionID, *seq)
			}

		case OpcodeDispatch:
			// set last sequence received
			sequence := message.S
			g.stateMu.Lock()
			g.config.LastSequenceReceived = &sequence
			g.stateMu.Unlock()

			eventData, ok := message.D.(EventData)
			if !ok && message.D != nil {
				g.config.Logger.Error("invalid message data received", slog.String("data", fmt.Sprintf("%T", message.D)))
				continue
			}

			// get session id here
			if readyEvent, ok := eventData.(EventReady); ok {
				g.stateMu.Lock()
				g.config.SessionID = &readyEvent.SessionID
				g.config.ResumeURL = &readyEvent.ResumeGatewayURL
				g.stateMu.Unlock()
				g.setStatus(conn, StatusReady)
				g.config.Logger.Debug("ready message received")
			}

			if unknownEvent, ok := eventData.(EventUnknown); ok {
				g.config.Logger.Debug("unknown event received", slog.String("event", string(message.T)), slog.String("data", string(unknownEvent)))
				continue
			}

			// push message to the command manager
			if g.config.EnableRawEvents {
				g.eventHandlerFunc(EventTypeRaw, message.S, g.config.ShardID, EventRaw{
					EventType: message.T,
					Payload:   bytes.NewReader(message.RawD),
				})
			}
			g.eventHandlerFunc(message.T, message.S, g.config.ShardID, eventData)

		case OpcodeHeartbeat:
			g.sendHeartbeat(conn, run)

		case OpcodeReconnect:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			closed := g.closeConn(ctx, conn, websocket.CloseServiceRestart, "received reconnect")
			cancel()
			if closed {
				g.endedByGateway(run, ErrReconnectRequested)
			}
			break loop

		case OpcodeInvalidSession:
			canResume := message.D.(MessageDataInvalidSession)

			code := websocket.CloseNormalClosure
			endErr := ErrInvalidSession
			if canResume {
				code = websocket.CloseServiceRestart
				endErr = ErrInvalidSessionResumable
			} else {
				// clear resume info
				g.clearSession()
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			closed := g.closeConn(ctx, conn, code, "invalid session")
			cancel()
			if closed {
				g.endedByGateway(run, endErr)
			}
			break loop

		case OpcodeHeartbeatACK:
			newHeartbeat := time.Now().UTC()
			g.stateMu.Lock()
			lastHeartbeat := g.lastHeartbeatReceived
			g.lastHeartbeatReceived = newHeartbeat
			g.stateMu.Unlock()
			g.eventHandlerFunc(EventTypeHeartbeatAck, message.S, g.config.ShardID, EventHeartbeatAck{
				LastHeartbeat: lastHeartbeat,
				NewHeartbeat:  newHeartbeat,
			})

		default:

			g.config.Logger.Debug("unknown opcode received", slog.Int("opcode", int(message.Op)), slog.String("data", fmt.Sprintf("%s", message.D)))
		}
	}
}

func (g *gatewayImpl) parseMessage(mt int, r io.Reader) (Message, error) {
	if mt == websocket.BinaryMessage {
		g.config.Logger.Debug("binary message received. decompressing")

		reader, err := zlib.NewReader(r)
		if err != nil {
			return Message{}, fmt.Errorf("failed to decompress zlib: %w", err)
		}
		defer reader.Close()
		r = reader
	}

	if g.config.Logger.Enabled(context.Background(), slog.LevelDebug) {
		buff := new(bytes.Buffer)
		tr := io.TeeReader(r, buff)
		data, err := io.ReadAll(tr)
		if err != nil {
			return Message{}, fmt.Errorf("failed to read message: %w", err)
		}
		g.config.Logger.Debug("received gateway message", slog.String("data", string(data)))
		r = buff
	}

	var message Message
	return message, json.NewDecoder(r).Decode(&message)
}
