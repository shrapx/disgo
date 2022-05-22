package handlers

import (
	"github.com/disgoorg/disgo/bot"
	"github.com/disgoorg/disgo/discord"
	"github.com/disgoorg/disgo/events"
)

// gatewayHandlerGuildMemberUpdate handles discord.GatewayEventTypeGuildMemberUpdate
type gatewayHandlerGuildMemberUpdate struct{}

// EventType returns the discord.GatewayEventType
func (h *gatewayHandlerGuildMemberUpdate) EventType() discord.GatewayEventType {
	return discord.GatewayEventTypeGuildMemberUpdate
}

// New constructs a new payload receiver for the raw gateway event
func (h *gatewayHandlerGuildMemberUpdate) New() any {
	return &discord.Member{}
}

// HandleGatewayEvent handles the specific raw gateway event
func (h *gatewayHandlerGuildMemberUpdate) HandleGatewayEvent(client bot.Client, sequenceNumber int, shardID int, v any) {
	member := *v.(*discord.Member)

	oldMember, _ := client.Caches().Members().Get(member.GuildID, member.User.ID)
	client.Caches().Members().Put(member.GuildID, member.User.ID, member)

	client.EventManager().DispatchEvent(&events.GuildMemberUpdate{
		GenericGuildMember: &events.GenericGuildMember{
			GenericEvent: events.NewGenericEvent(client, sequenceNumber, shardID),
			GuildID:      member.GuildID,
			Member:       member,
		},
		OldMember: oldMember,
	})
}