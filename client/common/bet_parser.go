package common

import (
	"encoding/csv"
	"io"
	"os"
)

type BetParser struct {
	scanner *csv.Reader
	file    *os.File
}

func NewBetParser(filePath string) (*BetParser, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = 5

	return &BetParser{scanner: reader, file: file}, nil
}

func (r *BetParser) ReadBets(batchSize int) ([]Bet, error) {
	var bets []Bet
	for i := 0; i < batchSize; i++ {
		record, err := r.scanner.Read()
		if err == io.EOF {
			if len(bets) > 0 {
				return bets, nil
			}
			return nil, io.EOF
		}
		if err != nil {
			return nil, err
		}
		bets = append(bets, Bet{
			Name:      record[0],
			Surname:   record[1],
			ID:        record[2],
			Birthdate: record[3],
			Number:    record[4],
		})
	}
	return bets, nil
}

func (r *BetParser) Close() {
	r.file.Close()
}
