package common

import (
	"bufio"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

const (
	DataMessageType = 0x01
	EndMessageType  = 0x02
	AskWinnersType  = 0x03

	BetsDataPath = "./agency_bets.csv"
)

var log = logging.MustGetLogger("log")

// AgencyConfig holds the configuration for an Agency
type AgencyConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	BatchSize     int
}

// Agency represents an agency instance
type Agency struct {
	config    AgencyConfig
	conn      net.Conn
	betParser *BetParser
}

// ---------------- Initialization ----------------

// NewAgency creates a new Agency with config, bet parser and shutdown handler
func NewAgency(config AgencyConfig) *Agency {
	betParser, err := NewBetParser(BetsDataPath)
	if err != nil {
		log.Criticalf("action: initialize_bet_parser | result: fail | error: %v", err)
		os.Exit(1)
	}
	agency := &Agency{
		config:    config,
		betParser: betParser,
	}
	agency.handleShutdown()
	return agency
}

// handleShutdown listens for SIGTERM and gracefully shuts down the agency
func (a *Agency) handleShutdown() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	go func() {
		<-sigs
		a.Close()
		os.Exit(0)
	}()
}

// ---------------- Connection ----------------

// createAgencySocket connects the agency to the server
func (a *Agency) createAgencySocket() error {
	conn, err := net.Dial("tcp", a.config.ServerAddress)
	if err != nil {
		log.Criticalf("action: connect | result: fail | agency_id: %v | error: %v",
			a.config.ID, err)
		if a.betParser != nil {
			a.betParser.Close()
		}
		return err
	}
	a.conn = conn
	return nil
}

// ---------------- Messaging ----------------

// sendMessage writes a full message to the server (handling partial writes)
func (a *Agency) sendMessage(message []byte) error {
	totalWritten := 0
	for totalWritten < len(message) {
		n, err := a.conn.Write(message[totalWritten:])
		if err != nil {
			return err
		}
		totalWritten += n
	}
	return nil
}

// ---------------- Bets ----------------

// sendBets reads bets from file and sends them in batches
func (a *Agency) sendBets() {
	for {
		bets, err := a.betParser.ReadBets(a.config.BatchSize, a.config.ID)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Errorf("action: read_bet | result: fail | error: %v", err)
			a.Close()
		}

		err = a.sendMessage(serializeBets(bets))
		if err != nil {
			log.Errorf("action: apuesta_enviada | result: fail | error: %v", err)
			a.Close()
		}
	}
	a.sendMessage([]byte{EndMessageType})
}

// ---------------- Winners ----------------

// receiveWinners asks server for winners and processes the response
func (a *Agency) receiveWinners() {
	a.sendMessage([]byte{AskWinnersType})

	reader := bufio.NewReader(a.conn)
	var winnersAmount = 0

	for {
		msgType, err := reader.ReadByte()
		if err != nil {
			log.Errorf("action: receive_message | result: fail | agency_id: %v | error: %v",
				a.config.ID, err)
			return
		}

		if msgType == EndMessageType {
			break
		}

		if msgType == DataMessageType {
			_, err := reader.ReadString('\n')
			if err != nil {
				log.Errorf("action: receive_message | result: fail | agency_id: %v | error: %v",
					a.config.ID, err)
				return
			}
			winnersAmount++
		}
	}

	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %v", winnersAmount)
}

// ---------------- Lifecycle ----------------

// StartAgency runs the agency lifecycle
func (a *Agency) StartAgency() {
	if err := a.createAgencySocket(); err != nil {
		os.Exit(1)
	}
	a.sendBets()
	a.receiveWinners()
	a.Close()
}

// Close frees all resources
func (a *Agency) Close() {
	if a.conn != nil {
		a.conn.Close()
	}
	if a.betParser != nil {
		a.betParser.Close()
	}
	log.Infof("action: close | result: success | agency_id: %v", a.config.ID)
}
