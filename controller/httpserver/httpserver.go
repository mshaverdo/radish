package httpserver

import (
	"bytes"
	"context"
	"fmt"
	"github.com/mshaverdo/radish/log"
	"github.com/mshaverdo/radish/message"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"strings"
)

// HttpServer is a implementation of HttpServer interface
type HttpServer struct {
	http.Server
	messageHandler MessageHandler
}

// MessageHandler processes a Request message and return a response message
type MessageHandler interface {
	HandleMessage(request *message.Request) *message.Response
}

// New Returns new instance of Radish HTTP server
func New(host string, port int, messageHandler MessageHandler) *HttpServer {
	// use server instance instead of http.ListenAndServe -- due to we should use graceful shutdown
	addr := fmt.Sprintf("%s:%d", host, port)

	s := HttpServer{
		Server:         http.Server{Addr: addr},
		messageHandler: messageHandler,
	}

	serverMux := http.NewServeMux()
	serverMux.Handle("/", &s)
	s.Server.Handler = serverMux

	return &s
}

// ListenAndServe statrs listening to incoming connections
func (s *HttpServer) ListenAndServe() error {
	if err := s.Server.ListenAndServe(); err == http.ErrServerClosed {
		return nil
	} else {
		return err
	}
}

// Shutdown gracefully shuts server down
func (s *HttpServer) Shutdown() error {
	return s.Server.Shutdown(context.TODO())
}

// ServeHTTP handles all requests to Http API.
// ServeHTTP transforms HTTP request into a message.Request,
// sends it to MessageHandler, waits until message processed,
// receives message.Response, corresponding to sent Request
// and transorms message.Response into HTTP response
func (s *HttpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	//TODO: if header isn't multipart, extract single value from body, if it is
	var (
		request  *message.Request
		response *message.Response
	)
	mr, err := r.MultipartReader()
	if err == http.ErrNotMultipart {
		// this is single-payload request
		request, err = s.parseSinglePayload(r)
	} else {
		request, err = s.parseMultiPayload(r, mr)
	}

	if err != nil {
		http.Error(w, "Error during processing request: "+err.Error(), http.StatusBadRequest)
		return
	}

	//TODO: добавить маппинг статусов response на статусы HTTP

	response = s.messageHandler.HandleMessage(request)

	if len(response.MultiPayloads) > 0 {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for key, val := range response.MultiPayloads {
			_ = writer.WriteField(key, string(val))
		}
		err := writer.Close()
		if err != nil {
			log.Errorf("Error writing multipart response: %s", err.Error())
			http.Error(w, "Error during processing request: "+err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", writer.FormDataContentType())
		io.Copy(w, body)
	} else {
		w.Write(response.Payload)
	}

}

func (s *HttpServer) parseSinglePayload(r *http.Request) (req *message.Request, err error) {
	//TODO: add checks
	urlparts := strings.Split(r.URL.Path, "/")
	payload, err := ioutil.ReadAll(r.Body)
	return message.NewRequest(urlparts[0], urlparts[1:], nil, payload, nil), err
}

func (s *HttpServer) parseMultiPayload(r *http.Request, mr *multipart.Reader) (req *message.Request, err error) {
	//TODO: add checks
	//TODO: fix parsing FormName() with \n in the middle
	urlparts := strings.Split(r.URL.Path, "/")

	multipayload := map[string][]byte{}
	for p, err := mr.NextPart(); err == nil; p, err = mr.NextPart() {
		payload, err := ioutil.ReadAll(p)

		if err != nil {
			log.Errorf("Error reading part: %s...", err.Error())
			return nil, err
		}

		log.Debugf("Got payload: %s =  %s", p.FormName(), string(payload))
		multipayload[p.FormName()] = payload
	}

	return message.NewRequest(urlparts[0], urlparts[1:], nil, nil, multipayload), err
}
