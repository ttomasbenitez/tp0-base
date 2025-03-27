package common

import (
	"encoding/binary"
	"fmt"
)

type Bet struct {
	Agency    string
	Name      string
	Surname   string
	ID        string
	Birthdate string
	Number    string
}

// This function serializes a slice of Bet structs into a TLV-encoded byte slice.
// Each bet is separated by a \n character, and each field is separated by a | character.
// The TLV format consists of a single byte for the type, two bytes for the length,
// followed by the actual data.
func serializeBets(bets []Bet) []byte {
	var messageData string

	for _, bet := range bets {
		line := fmt.Sprintf("%s|%s|%s|%s|%s|%s\n", bet.Agency, bet.Name, bet.Surname, bet.ID, bet.Birthdate, bet.Number)
		messageData += line
	}

	valueBytes := []byte(messageData)
	messageLength := uint16(len(valueBytes))

	// TLV: Type (1 byte) + Length (2 bytes) + Value
	message := make([]byte, 3)
	message[0] = DataMessageType // Type: Bet data
	binary.BigEndian.PutUint16(message[1:], messageLength)
	message = append(message, valueBytes...)

	return message
}
