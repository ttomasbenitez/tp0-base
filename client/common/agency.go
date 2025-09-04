package common

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

// AgencyConfig Configuration used by the agency
type AgencyConfig struct {
	ID            string
	ServerAddress string
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
	config   AgencyConfig
	conn     net.Conn
	bet      Bet
	mutex    sync.Mutex
	shutdown bool
}

func (a *Agency) handleShutdown() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM)

	go func() {
		<-sigs
		a.mutex.Lock()
		a.shutdown = true
		a.mutex.Unlock()

		log.Infof("action: shutdown | result: success | client_id: %v", a.config.ID)
		if a.conn != nil {
			a.conn.Close()
		}
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
func (a *Agency) createAgencySocket() error {
	conn, err := net.Dial("tcp", a.config.ServerAddress)
	if err != nil {
		log.Criticalf(
			"action: connect | result: fail | client_id: %v | error: %v",
			a.config.ID,
			err,
		)
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

func (a *Agency) sendBet() error {
	messageData := fmt.Sprintf(
		"%s|%s|%s|%s|%s\n",
		a.bet.Name,
		a.bet.Surname,
		a.bet.ID,
		a.bet.Birthdate,
		a.bet.Number,
	)

	messageLength := uint8(len(messageData))
	message := append([]byte{messageLength}, []byte(messageData)...)

	totalWritten := 0
	for totalWritten < len(message) {
		n, err := a.conn.Write(message[totalWritten:])
		if err != nil {
			return fmt.Errorf("failed to send bet: %w", err)
		}
		totalWritten += n
	}
	return nil
}

func (a *Agency) receiveResponse() (string, error) {
	msg, err := bufio.NewReader(a.conn).ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("failed to read response: %w", err)
	}
	return msg, nil
}

func (a *Agency) StartAgency() {
	if err := a.createAgencySocket(); err != nil {
		log.Errorf("action: connect | result: fail | agency_id: %v | error: %v", a.config.ID, err)
		return
	}
	defer a.conn.Close()

	if a.isShutdown() {
		return
	}

	if err := a.sendBet(); err != nil {
		log.Errorf("action: apuesta_enviada | result: fail | dni: %v | error: %v", a.bet.ID, err)
		return
	}

	msg, err := a.receiveResponse()
	if err != nil {
		log.Errorf("action: receive_message | result: fail | agency_id: %v | error: %v", a.config.ID, err)
		return
	}

	log.Infof("action: apuesta_enviada | result: success | dni: %v | numero: %v", a.bet.ID, msg)
}
