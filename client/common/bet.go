package common

import (
	"encoding/binary"
	"fmt"
)

type Bet struct {
	Name      string
	Surname   string
	ID        string
	Birthdate string
	Number    string
}

func serializeBets(bets []Bet) []byte {
	var messageData string

	for _, bet := range bets {
		line := fmt.Sprintf("%s|%s|%s|%s|%s\n", bet.Name, bet.Surname, bet.ID, bet.Birthdate, bet.Number)
		messageData += line
	}

	valueBytes := []byte(messageData)
	messageLength := uint16(len(valueBytes))

	// TLV: Type (1 byte) + Length (2 bytes) + Value
	message := make([]byte, 3)
	message[0] = 0x01 // Type: Bet data
	binary.BigEndian.PutUint16(message[1:], messageLength)
	message = append(message, valueBytes...)

	return message
}
