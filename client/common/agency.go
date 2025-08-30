package common

import (
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
}

func (c *Agency) handleShutdown() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	go func() {
		<-sigs
		log.Infof("action: shutdown | result: success | agency_id: %v", c.config.ID)
		if c.conn != nil {
			c.conn.Close()
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
func (c *Agency) createAgencySocket() error {
	conn, err := net.Dial("tcp", c.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
	}
	c.conn = conn
	return nil
}

func (c *Agency) sendMessage(message []byte) error {
	totalWritten := 0
	for totalWritten < len(message) {
		n, err := c.conn.Write(message[totalWritten:])
		if err != nil {
			return err
		}
		totalWritten += n
	}
	log.Infof("action: ENVIADO | result: success | BYTES: %v", totalWritten)
	return nil
}

func (c *Agency) waitForServerConfirmation() error {
	buf := make([]byte, 1)
	_, err := c.conn.Read(buf)
	if err != nil {
		return err
	}
	if buf[0] != EndMessageType {
		return fmt.Errorf("unexpected confirmation byte: %v", buf[0])
	}
	log.Infof("action: confirmation_received | result: success")
	return nil
}

func (c *Agency) StartAgency() {
	if err := c.createAgencySocket(); err != nil {
		log.Errorf("action: connect | result: fail | agency_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return
	}

	defer func() {
		if tcpConn, ok := c.conn.(*net.TCPConn); ok {
			tcpConn.CloseWrite()
		}
		c.conn.Close()
	}()

	for {
		log.Infof("action: READ | result: success")
		bets, err := c.betParser.ReadBets(c.config.BatchSize)
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Errorf("action: read_bet | result: fail | error: %v", err)
			return
		}

		err = c.sendMessage(serializeBets(bets))
		if err != nil {
			log.Errorf("action: apuesta_enviada | result: fail | error: %v", err)
			return
		}
		log.Infof("action: SENT | result: success")
	}
	c.sendMessage([]byte{EndMessageType})

	if err := c.waitForServerConfirmation(); err != nil {
		log.Errorf("action: confirmation | result: fail | error: %v", err)
	}

	log.Infof("action: apuesta_enviada | result: success")
	c.betParser.Close()
	time.Sleep(300 * time.Millisecond)
}
