package discord

import "github.com/DisgoOrg/disgo/json"

type ApplicationCommandCreate interface {
	json.Marshaler
	Type() ApplicationCommandType
	applicationCommandCreate()
}

type SlashCommandCreate struct {
	Name              string                     `json:"name"`
	Description       string                     `json:"description"`
	Options           []ApplicationCommandOption `json:"options,omitempty"`
	DefaultPermission bool                       `json:"default_permission,omitempty"`
}

func (c SlashCommandCreate) MarshalJSON() ([]byte, error) {
	type slashCommandCreate SlashCommandCreate
	v := struct {
		Type ApplicationCommandType `json:"type"`
		slashCommandCreate
	}{
		Type:               c.Type(),
		slashCommandCreate: slashCommandCreate(c),
	}
	return json.Marshal(v)
}

func (_ SlashCommandCreate) Type() ApplicationCommandType {
	return ApplicationCommandTypeSlash
}

func (_ SlashCommandCreate) applicationCommandCreate() {}

type UserCommandCreate struct {
	Name              string `json:"name"`
	DefaultPermission bool   `json:"default_permission,omitempty"`
}

func (c UserCommandCreate) MarshalJSON() ([]byte, error) {
	type userCommandCreate UserCommandCreate
	v := struct {
		Type        ApplicationCommandType `json:"type"`
		Description string                 `json:"description"`
		userCommandCreate
	}{
		Type:              c.Type(),
		userCommandCreate: userCommandCreate(c),
	}
	return json.Marshal(v)
}

func (_ UserCommandCreate) Type() ApplicationCommandType {
	return ApplicationCommandTypeUser
}

func (_ UserCommandCreate) applicationCommandCreate() {}

type MessageCommandCreate struct {
	Name              string `json:"name"`
	DefaultPermission bool   `json:"default_permission,omitempty"`
}

func (c MessageCommandCreate) MarshalJSON() ([]byte, error) {
	type messageCommandCreate MessageCommandCreate
	v := struct {
		Type        ApplicationCommandType `json:"type"`
		Description string                 `json:"description"`
		messageCommandCreate
	}{
		Type:                 c.Type(),
		messageCommandCreate: messageCommandCreate(c),
	}
	return json.Marshal(v)
}

func (_ MessageCommandCreate) Type() ApplicationCommandType {
	return ApplicationCommandTypeMessage
}

func (_ MessageCommandCreate) applicationCommandCreate() {}
