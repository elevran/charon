package main

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func createMsg(model string, input interface{}, options ...map[string]interface{}) map[string]interface{} {
	msg := map[string]interface{}{
		"type":  "response.create",
		"model": model,
		"input": input,
	}
	if len(options) > 0 {
		for k, v := range options[0] {
			msg[k] = v
		}
	}
	return msg
}

func TestWSBasicResponse(t *testing.T) {
	s := newTestStack(t)
	ws := dialWS(t, s.proxyURL)

	ws.send(createMsg("test", "hello"))
	resp, errCode := ws.readUntil(5 * time.Second)

	assert.Empty(t, errCode)
	assert.Equal(t, "completed", resp.Status)
	assert.NotEmpty(t, resp.ID)
	assert.NotEmpty(t, resp.Output)
}
