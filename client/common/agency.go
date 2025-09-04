package common

import (
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
	EndMessageType = 0x02
	MaxBatchSize   = 8 * 1024 // 8 KB
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
	mutex     sync.Mutex
	shutdown  bool
}

func (a *Agency) handleShutdown() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	go func() {
		<-sigs
		a.mutex.Lock()
		a.shutdown = true
		a.mutex.Unlock()

		log.Infof("action: shutdown | result: success | agency_id: %v", a.config.ID)
		if a.conn != nil {
			a.conn.Close()
		}
		if a.betParser != nil {
			a.betParser.Close()
		}
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

func (a *Agency) isShutdown() bool {
	a.mutex.Lock()
	defer a.mutex.Unlock()
	return a.shutdown
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
	log.Infof("action: ENVIADO | result: success | BYTES: %v", totalWritten)
	return nil
}

func (a *Agency) processBets() error {
	for {
		bets, err := a.betParser.ReadBets(a.config.BatchSize)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return fmt.Errorf("failed to read bets: %w", err)
		}
		log.Infof("action: bets_read | result: success | count: %d", len(bets))

		if err := a.sendMessage(serializeBets(bets)); err != nil {
			return fmt.Errorf("failed to send batch: %w", err)
		}
		log.Infof("action: batch_sent | result: success")
	}
}

func (a *Agency) StartAgency() {
	if err := a.createAgencySocket(); err != nil {
		log.Errorf("action: connect | result: fail | agency_id: %v | error: %v", a.config.ID, err)
		return
	}

	defer func() {
		a.betParser.Close()
		if tcpConn, ok := a.conn.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
		a.conn.Close()
	}()

	if a.isShutdown() {
		return
	}

	if err := a.processBets(); err != nil {
		log.Errorf("action: process_bets | result: fail | error: %v", err)
		return
	}

	if err := a.sendMessage([]byte{EndMessageType}); err != nil {
		log.Errorf("action: send_end_message | result: fail | error: %v", err)
		return
	}

	log.Infof("action: apuesta_enviada | result: success")
}
