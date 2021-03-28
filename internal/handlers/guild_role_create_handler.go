package handlers

import (
	"github.com/DisgoOrg/disgo/api"
	"github.com/DisgoOrg/disgo/api/events"
)

type roleCreateData struct {
	GuildID api.Snowflake `json:"guild_id"`
	Role    *api.Role     `json:"role"`
}

// GuildRoleCreateHandler handles api.GuildRoleCreateGatewayEvent
type GuildRoleCreateHandler struct{}

// Name returns the raw gateway event name
func (h GuildRoleCreateHandler) Name() string {
	return api.GuildRoleCreateGatewayEvent
}

// New constructs a new payload receiver for the raw gateway event
func (h GuildRoleCreateHandler) New() interface{} {
	return &roleCreateData{}
}

// Handle handles the specific raw gateway event
func (h GuildRoleCreateHandler) Handle(disgo api.Disgo, eventManager api.EventManager, i interface{}) {
	roleCreateData, ok := i.(*roleCreateData)
	if !ok {
		return
	}
	roleCreateData.Role.Disgo = disgo
	roleCreateData.Role.GuildID = roleCreateData.GuildID
	disgo.Cache().CacheRole(roleCreateData.Role)

	genericGuildEvent := events.GenericGuildEvent{
		Event: api.Event{
			Disgo: disgo,
		},
		GuildID: roleCreateData.GuildID,
	}
	eventManager.Dispatch(genericGuildEvent)

	genericRoleEvent := events.GenericGuildRoleEvent{
		GenericGuildEvent: genericGuildEvent,
		Role:              roleCreateData.Role,
		RoleID:            roleCreateData.Role.ID,
	}
	eventManager.Dispatch(genericRoleEvent)

	eventManager.Dispatch(events.GuildRoleCreateEvent{
		GenericGuildEvent: genericGuildEvent,
	})
}