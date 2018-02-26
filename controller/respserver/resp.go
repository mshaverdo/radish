package respserver

import (
	"fmt"
	"github.com/mshaverdo/radish/log"
	"github.com/mshaverdo/radish/message"
	"github.com/tidwall/redcon"
	"strings"
)

type RespServer struct {
	host           string
	port           int
	server         *redcon.Server
	messageHandler MessageHandler
	stopChan       chan struct{}
}

//TODO: возвращать ошибку на неизвестные команды
//TODO: этот интерфейс уже определен в httpserver. решить ,чт ос ним делать.
//TODO: set expiration! должен корректно обрабатывать команды SET <key> <value> EX <seconds>
// MessageHandler processes a Request message and return a response message
type MessageHandler interface {
	HandleMessage(request *message.Request) message.Response
}

// New Returns new instance of RespServer
func New(host string, port int, messageHandler MessageHandler) *RespServer {
	s := RespServer{
		messageHandler: messageHandler,
		stopChan:       make(chan struct{}),
		host:           host,
		port:           port,
	}

	return &s
}

// ListenAndServe statrs listening to incoming connections
func (s *RespServer) ListenAndServe() error {
	s.server = redcon.NewServerNetwork(
		"tcp",
		fmt.Sprintf("%s:%d", s.host, s.port),
		s.handler,
		nil, //func(conn redcon.Conn) bool { return true },
		nil,
	)

	err := s.server.ListenAndServe()

	if err == nil {
		<-s.stopChan // wait for full shutdown
		return nil
	} else {
		return err
	}
}

// Stops accepting new requests by Resp server, but not causes return from ListenAndServe() until Shutdown()
func (s *RespServer) Stop() error {
	return s.server.Close()
}

// Shutdown gracefully shuts server down
func (s *RespServer) Shutdown() error {
	defer close(s.stopChan)
	return s.Stop()
}

func (s *RespServer) handler(conn redcon.Conn, command redcon.Command) {
	//TODO: проверить пайплайн!

	argsCount := len(command.Args)
	if argsCount == 0 {
		// redcon souldn't pass empty commands here, but...
		return
	}

	cmd := strings.ToUpper(string(command.Args[0]))
	// handle some RESP-level service commands here
	switch cmd {
	case "PING":
		conn.WriteString("PONG")
		return
	case "QUIT":
		conn.WriteString("OK")
		conn.Close()
		return
	}

	log.Debugf("Received request: %q", command.Args)

	request := message.NewRequest(cmd, command.Args[1:])

	log.Debugf("Handling request: %s", request)

	response := s.messageHandler.HandleMessage(request)

	log.Debugf("Sending response: %s", response)

	err := sendResponse(response, conn)
	if err != nil {
		log.Errorf("Sending response failed: %s", err)
	}
}

func sendResponse(response message.Response, conn redcon.Conn) error {
	switch concreteResponse := response.(type) {
	case *message.ResponseStatus:
		switch concreteResponse.Status() {
		case message.StatusOk:
			conn.WriteString("OK")
		case message.StatusNotFound:
			conn.WriteNull()
		case message.StatusTypeMismatch:
			conn.WriteError("WRONGTYPE Operation against a key holding the wrong kind of value")
		default:
			conn.WriteError("ERR " + concreteResponse.Payload())
		}
	case *message.ResponseString:
		conn.WriteBulk(concreteResponse.Payload())
	case *message.ResponseStringSlice:
		conn.WriteArray(len(concreteResponse.Payload()))
		for _, v := range concreteResponse.Payload() {
			conn.WriteBulk(v)
		}
	case *message.ResponseInt:
		conn.WriteInt(concreteResponse.Payload())
	default:
		return fmt.Errorf("unknown response type: %T", response)
	}

	return nil
}
