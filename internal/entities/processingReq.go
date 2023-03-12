package entities

import (
	"github.com/google/uuid"
	"time"
)

type ProcessingRequest struct {
	Payload          []byte
	Timestamp        time.Time
	RequestID        uuid.UUID
	ClientID         string
	TypeOfPrediction string
	MessageType      string
}
