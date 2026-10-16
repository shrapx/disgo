package handlers_test

import (
	"testing"

	"github.com/disgoorg/json"
	"github.com/stretchr/testify/require"

	"github.com/disgoorg/disgo"
	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/events"
	"github.com/disgoorg/disgo/gateway"
	"github.com/disgoorg/disgo/handlers"
)

// a token whose first part is base64("123456789012345678"); no connection is made
const testToken = "MTIzNDU2Nzg5MDEyMzQ1Njc4.x.y"

// dispatch decodes a gateway payload and hands it to the client the way gatewayImpl.listen does for opcode 0.
func dispatch(t *testing.T, client bot.Client, payload string) {
	t.Helper()
	var message gateway.Message
	require.NoError(t, json.Unmarshal([]byte(payload), &message))
	require.Equal(t, gateway.OpcodeDispatch, message.Op)

	eventData, ok := message.D.(gateway.EventData)
	if !ok && message.D != nil {
		t.Fatalf("invalid message data: %T", message.D)
	}
	handlers.DefaultGatewayEventHandlerFunc(client)(message.T, message.S, 0, eventData)
}

func TestResumedDispatchesEvent(t *testing.T) {
	payloads := map[string]string{
		"null data":  `{"op":0,"s":42,"t":"RESUMED","d":null}`,
		"trace data": `{"op":0,"s":42,"t":"RESUMED","d":{"_trace":["[\"gateway-prd-main-abcd\",{\"micros\":1234}]"]}}`,
		"no data":    `{"op":0,"s":42,"t":"RESUMED"}`,
	}
	for name, payload := range payloads {
		t.Run(name, func(t *testing.T) {
			var resumed []*events.Resumed
			client, err := disgo.New(testToken, bot.WithEventListenerFunc(func(e *events.Resumed) {
				resumed = append(resumed, e)
			}))
			require.NoError(t, err)

			dispatch(t, client, payload)

			require.Len(t, resumed, 1, "RESUMED must dispatch events.Resumed")
			require.Equal(t, 42, resumed[0].SequenceNumber())
			require.Equal(t, 0, resumed[0].ShardID())
		})
	}
}

// TestReadyDispatchesEvent is the control for TestResumedDispatchesEvent: the same path delivers READY.
func TestReadyDispatchesEvent(t *testing.T) {
	var ready []*events.Ready
	client, err := disgo.New(testToken, bot.WithEventListenerFunc(func(e *events.Ready) {
		ready = append(ready, e)
	}))
	require.NoError(t, err)

	dispatch(t, client, `{"op":0,"s":1,"t":"READY","d":{"v":10,"session_id":"abc","resume_gateway_url":"wss://example.invalid","user":{"id":"123456789012345678"},"guilds":[]}}`)

	require.Len(t, ready, 1)
	require.Equal(t, "abc", ready[0].SessionID)
}
