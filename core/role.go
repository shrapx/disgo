package core

import (
	"context"

	"github.com/DisgoOrg/disgo/discord"
	"github.com/DisgoOrg/disgo/rest"
)

type Role struct {
	discord.Role
	Disgo Disgo
}

// Mention parses the Role as a Mention
func (r *Role) Mention() string {
	return "<@&" + r.ID.String() + ">"
}

// String parses the Role to a String representation
func (r *Role) String() string {
	return r.Mention()
}

// Guild returns the Guild of this role from the Cache
func (r *Role) Guild() *Guild {
	return r.Disgo.Cache().GuildCache().Get(r.GuildID)
}

// Update updates the Role with specific values
func (r *Role) Update(ctx context.Context, roleUpdate discord.RoleUpdate) (*Role, rest.Error) {
	role, err := r.Disgo.RestServices().GuildService().UpdateRole(ctx, r.GuildID, r.ID, roleUpdate)
	if err != nil {
		return nil, err
	}
	return r.Disgo.EntityBuilder().CreateRole(r.GuildID, *role, CacheStrategyNoWs), nil
}

// SetPosition sets the position of the Role
func (r *Role) SetPosition(ctx context.Context, rolePositionUpdate discord.RolePositionUpdate) ([]*Role, rest.Error) {
	roles, err := r.Disgo.RestServices().GuildService().UpdateRolePositions(ctx, r.GuildID, rolePositionUpdate)
	if err != nil {
		return nil, err
	}
	coreRoles := make([]*Role, len(roles))
	for i, role := range roles {
		coreRoles[i] = r.Disgo.EntityBuilder().CreateRole(r.GuildID, role, CacheStrategyNoWs)
	}
	return coreRoles, nil
}

// Delete deletes the Role
func (r *Role) Delete(ctx context.Context) rest.Error {
	return r.Disgo.RestServices().GuildService().DeleteRole(ctx, r.GuildID, r.ID)
}
