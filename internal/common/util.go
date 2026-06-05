package common

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

func GenerateID(prefix string) string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	if prefix != "" {
		return prefix + "-" + hex.EncodeToString(b)
	}
	return hex.EncodeToString(b)
}

func GenerateRunID() string {
	return GenerateID("run")
}

func GenerateWorkflowID() string {
	return GenerateID("wf")
}

func GenerateActivityID() string {
	return GenerateID("act")
}

func GenerateTimerID() string {
	return GenerateID("timer")
}

func ToJSON(v interface{}) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func FromJSON(data json.RawMessage, v interface{}) error {
	return json.Unmarshal(data, v)
}

func Now() time.Time {
	return time.Now().UTC()
}

func ParseDuration(s string) (time.Duration, error) {
	return time.ParseDuration(s)
}
