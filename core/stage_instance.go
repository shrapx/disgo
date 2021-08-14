package core

import (
	"context"

	"github.com/DisgoOrg/disgo/discord"
	"github.com/DisgoOrg/disgo/rest"
)

type StageInstance struct {
	discord.StageInstance
	Disgo Disgo
}

func (i *StageInstance) Guild() *Guild {
	return i.Disgo.Cache().GuildCache().Get(i.GuildID)
}

func (i *StageInstance) Channel() StageChannel {
	return i.Disgo.Cache().StageChannelCache().Get(i.ChannelID)
}

func (i *StageInstance) Update(ctx context.Context, stageInstanceUpdate discord.StageInstanceUpdate) (*StageInstance, rest.Error) {
	stageInstance, err := i.Disgo.RestServices().StageInstanceService().UpdateStageInstance(ctx, i.ID, stageInstanceUpdate)
	if err != nil {
		return nil, err
	}
	return i.Disgo.EntityBuilder().CreateStageInstance(*stageInstance, CacheStrategyNoWs), nil
}

func (i *StageInstance) Delete(ctx context.Context) rest.Error {
	return i.Disgo.RestServices().StageInstanceService().DeleteStageInstance(ctx, i.ID)
}
