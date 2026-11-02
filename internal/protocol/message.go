package protocol

type MessageType int8

const (
	MessageTypeChat     MessageType = 1
	MessageTypeShutdown MessageType = 2
	MessageTypeACK      MessageType = 3
)

type Message struct {
	Type    MessageType
	Payload []byte
}

func NewMessage(messageType MessageType, message []byte) *Message {
	return &Message{
		Type:    messageType,
		Payload: message,
	}
}
