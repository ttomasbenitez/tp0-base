package common

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// AgencyConfig Configuration used by the agency
type AgencyConfig struct {
	ID            string
	ServerAddress string
	LoopAmount    int
	LoopPeriod    time.Duration
}

type Bet struct {
	Name      string
	Surname   string
	ID        string
	Birthdate string
	Number    string
}

// Agency Entity that encapsulates how
type Agency struct {
	config AgencyConfig
	conn   net.Conn
	bet    Bet
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
	agency := &Agency{
		config: config,
		bet: Bet{
			Name:      os.Getenv("NOMBRE"),
			Surname:   os.Getenv("APELLIDO"),
			ID:        os.Getenv("DOCUMENTO"),
			Birthdate: os.Getenv("NACIMIENTO"),
			Number:    os.Getenv("NUMERO"),
		},
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

func (c *Agency) makeBet() error {
	messageData := fmt.Sprintf("%s|%s|%s|%s|%s\n", c.bet.Name, c.bet.Surname, c.bet.ID, c.bet.Birthdate, c.bet.Number)

	messageLength := uint8(len(messageData))

	message := append([]byte{messageLength}, []byte(messageData)...)

	totalWritten := 0
	for totalWritten < len(message) {
		n, err := c.conn.Write(message[totalWritten:])
		if err != nil {
			return err
		}
		totalWritten += n
	}
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

	err := c.makeBet()
	if err != nil {
		log.Errorf("action: apuesta_enviada | result: fail | dni: %v | error: %v",
			c.bet.ID,
			err,
		)
		return
	}

	msg, err := bufio.NewReader(c.conn).ReadString('\n')
	if err != nil {
		log.Errorf("action: receive_message | result: fail | agency_id: %v | error: %v",
			c.config.ID,
			err,
		)
		c.conn.Close()
		return
	}
	log.Infof("action: apuesta_enviada | result: success | dni: %v | numero: %v", c.bet.ID, msg)

	c.conn.Close()
}
