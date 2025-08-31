package common

import (
	"bufio"
	"fmt"
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
	MaxBatchSize    = 8 * 1024 // 8 KB
)

var log = logging.MustGetLogger("log")

// AgencyConfig Configuration used by the agency
type AgencyConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
	BatchSize     int
}

// Agency Entity that encapsulates how
type Agency struct {
	config    AgencyConfig
	conn      net.Conn
	betParser *BetParser
}

func (a *Agency) handleShutdown() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Infof("action: shutdown | result: success | agency_id: %v", a.config.ID)
		if a.conn != nil {
			a.conn.Close()
		}
		if a.betParser != nil {
			a.betParser.Close()
		}
		os.Exit(0)
	}()
}

// NewAgency Initializes a new agency receiving the configuration
// as a parameter
func NewAgency(config AgencyConfig) *Agency {
	if config.BatchSize > MaxBatchSize {
		log.Warningf("BatchSize (%d) is greater than MaxBatchSize (%d). Setting to MaxBatchSize.", config.BatchSize, MaxBatchSize)
		config.BatchSize = MaxBatchSize
	}

	betParser, err := NewBetParser("./agency_bets.csv")
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

// CreateAgencySocket Initializes agency socket. In case of
// failure, error is printed in stdout/stderr and exit 1
// is returned
func (a *Agency) createAgencySocket() error {
	conn, err := net.Dial("tcp", a.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			a.config.ID,
			err,
		)
	}
	a.conn = conn
	return nil
}

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

func (a *Agency) processBets() error {
	for {
		bets, err := a.betParser.ReadBets(a.config.BatchSize, a.config.ID)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			log.Errorf("action: read_bet | result: fail | error: %v", err)
			return err
		}

		err = a.sendMessage(serializeBets(bets))
		if err != nil {
			log.Errorf("action: apuesta_enviada | result: fail | error: %v", err)
			return err
		}
	}
}

func (a *Agency) processWinners() (int, error) {
	reader := bufio.NewReader(a.conn)
	winnersAmount := 0

	for {
		msgType, err := reader.ReadByte()
		if err != nil {
			return 0, fmt.Errorf("failed to read message type: %w", err)
		}

		if msgType == EndMessageType {
			break
		}

		if msgType == DataMessageType {
			_, err := reader.ReadString('\n')
			if err != nil {
				return 0, fmt.Errorf("failed to read winner data: %w", err)
			}
			winnersAmount++
		}
	}

	return winnersAmount, nil
}

func (a *Agency) StartAgency() {
	if err := a.createAgencySocket(); err != nil {
		log.Errorf("action: connect | result: fail | agency_id: %v | error: %v", a.config.ID, err)
		return
	}

	defer func() {
		a.betParser.Close()
		a.conn.Close()
	}()

	if err := a.processBets(); err != nil {
		return
	}

	a.sendMessage([]byte{EndMessageType})
	a.sendMessage([]byte{AskWinnersType})

	winnersAmount, err := a.processWinners()
	if err != nil {
		log.Errorf("action: receive_message | result: fail | agency_id: %v | error: %v", a.config.ID, err)
		return
	}

	log.Infof("action: consulta_ganadores | result: success | cant_ganadores: %v", winnersAmount)
	//time.Sleep(300 * time.Millisecond)
}
