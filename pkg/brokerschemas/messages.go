package brokerschemas

import (
	"github.com/google/uuid"
	"time"
)

type NotificationMessage struct {
	NotificationMethods []string  `json:"notificationMethods"` // NotificationMethods - for example: telegram, vk, etc
	Payload             []byte    `json:"payload"`
	Timestamp           time.Time `json:"ts"`
	RequestID           uuid.UUID `json:"requestID"`
	ClientID            string    `json:"clientID"`
	MessageType         string    `json:"messageType"` // MessageType - audio or photo, text
}
