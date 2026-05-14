package model

import "encoding/json"

// ItemType is the discriminator field value.
type ItemType string

const (
	ItemTypeMessage            ItemType = "message"
	ItemTypeFunctionCall       ItemType = "function_call"
	ItemTypeFunctionCallOutput ItemType = "function_call_output"
	ItemTypeReasoning          ItemType = "reasoning"
	ItemTypeCompaction         ItemType = "compaction"
)

// Item is one element of a flat conversation context.
// The full JSON blob is preserved verbatim so encrypted_content fields
// on reasoning/compaction items pass through without inspection.
type Item struct {
	Type json.RawMessage `json:"type"`
	Raw  json.RawMessage `json:"-"` // full original JSON; set on unmarshal
}

// itemTypeOnly is used to peek at the type field without full unmarshal.
type itemTypeOnly struct {
	Type ItemType `json:"type"`
}

func (it *Item) ItemType() ItemType {
	var t itemTypeOnly
	_ = json.Unmarshal(it.Raw, &t)
	return t.Type
}
