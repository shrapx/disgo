package gateway

import "errors"

// The errors a SingleAttempt gateway passes to the CloseHandlerFunc when it ended the connection itself,
// instead of reconnecting. A close of the websocket (a close frame or a read error) is passed as it was read.
var (
	// ErrReconnectRequested: Discord sent opcode 7 (Reconnect). The session is kept, so a new connection can resume.
	ErrReconnectRequested = errors.New("gateway: discord requested a reconnect (opcode 7)")
	// ErrInvalidSession: Discord sent opcode 9 (Invalid Session) with d false. The session is gone: a new
	// connection has to identify, so the caller must not resume the session it holds.
	ErrInvalidSession = errors.New("gateway: invalid session, not resumable (opcode 9, d false)")
	// ErrInvalidSessionResumable: Discord sent opcode 9 (Invalid Session) with d true. The session is kept.
	ErrInvalidSessionResumable = errors.New("gateway: invalid session, resumable (opcode 9, d true)")
	// ErrHeartbeatFailed: sending a heartbeat failed. The session is kept. It wraps the send error.
	ErrHeartbeatFailed = errors.New("gateway: heartbeat failed")
)
