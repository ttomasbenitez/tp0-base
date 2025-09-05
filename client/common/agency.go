package common

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

const (
	DataMessageType = 0x01
	EndMessageType  = 0x02
	AskWinnersType  = 0x03
	BetsDataPath    = "./agency_bets.csv"
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
	mutex     sync.Mutex
	shutdown  bool
}

// NewAgency creates a new Agency with config, bet parser and shutdown handler
// Returns error if fails
func NewAgency(config AgencyConfig) (*Agency, error) {
	betParser, err := NewBetParser(BetsDataPath)
	if err != nil {
		log.Criticalf("action: initialize_bet_parser | result: fail | error: %v", err)
		return nil, fmt.Errorf("failed to initialize bet parser: %w", err)
	}

	agency := &Agency{
		config:    config,
		betParser: betParser,
	}
	agency.handleShutdown()
	return agency, nil
}

// handleShutdown listens for SIGTERM and gracefully shuts down the agency
func (a *Agency) handleShutdown() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		<-sigs
		a.mutex.Lock()
		a.shutdown = true
		a.mutex.Unlock()
		a.Close()
	}()
}

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

func (a *Agency) isShutdown() bool {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.shutdown
}

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

// sendBets reads bets from file and sends them in batches
func (a *Agency) sendBets() error {
	for {
		if a.isShutdown() {
			return nil
		}
		time.Sleep(6000 * time.Millisecond)

		bets, err := a.betParser.ReadBets(a.config.BatchSize, a.config.ID)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Errorf("action: read_bet | result: fail | error: %v", err)
			return fmt.Errorf("failed to read bets: %w", err)
		}

		err = a.sendMessage(serializeBets(bets))
		if err != nil {
			log.Errorf("action: apuesta_enviada | result: fail | error: %v", err)
			return fmt.Errorf("failed to send bets: %w", err)
		}
	}

	err := a.sendMessage([]byte{EndMessageType})
	if err != nil {
		return fmt.Errorf("failed to send end message: %w", err)
	}
	return nil
}

// receiveWinners asks server for winners and processes the response
func (a *Agency) receiveWinners() error {
	err := a.sendMessage([]byte{AskWinnersType})
	if err != nil {
		return fmt.Errorf("failed to send ask winners message: %w", err)
	}

	reader := bufio.NewReader(a.conn)
	var winnersAmount = 0

	for {
		if a.isShutdown() {
			return nil
		}

		msgType, err := reader.ReadByte()
		if err != nil {
			log.Errorf("action: receive_message | result: fail | agency_id: %v | error: %v",
				a.config.ID, err)
			return fmt.Errorf("failed to receive message: %w", err)
		}

		if msgType == EndMessageType {
			break
		}

		if msgType == DataMessageType {
			_, err := reader.ReadString('\n')
			if err != nil {
				log.Errorf("action: receive_message | result: fail | agency_id: %v | error: %v",
					a.config.ID, err)
				return fmt.Errorf("failed to read winner data: %w", err)
			}
			winnersAmount++
		}
	}

	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %v", winnersAmount)
	return nil
}

// StartAgency runs the agency lifecycle
func (a *Agency) StartAgency() error {
	defer a.Close()

	if err := a.createAgencySocket(); err != nil {
		return fmt.Errorf("failed to create socket: %w", err)
	}

	if err := a.sendBets(); err != nil {
		return fmt.Errorf("failed to send bets: %w", err)
	}

	if err := a.receiveWinners(); err != nil {
		return fmt.Errorf("failed to receive winners: %w", err)
	}

	return nil
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
