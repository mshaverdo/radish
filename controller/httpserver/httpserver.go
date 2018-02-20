package httpserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/mshaverdo/radish/log"
	"github.com/mshaverdo/radish/message"
	"io"
	"io/ioutil"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
)

const (
	StatusHeader = "X-Radish-Status"
)

// HttpServer is a implementation of HttpServer interface
type HttpServer struct {
	http.Server
	messageHandler MessageHandler
	stopChan       chan struct{}
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
		stopChan:       make(chan struct{}),
	}

	serverMux := http.NewServeMux()
	serverMux.Handle("/", &s)
	s.Server.Handler = serverMux

	return &s
}

// ListenAndServe statrs listening to incoming connections
func (s *HttpServer) ListenAndServe() error {
	if err := s.Server.ListenAndServe(); err == http.ErrServerClosed {
		<-s.stopChan // wait for full shutdown
		return nil
	} else {
		return err
	}
}

// Stops accepting new requests by HTTP server, but not causes return from ListenAndServe() until Shutdown()
func (s *HttpServer) Stop() error {
	return s.Server.Shutdown(context.TODO())
}

// Shutdown gracefully shuts server down
func (s *HttpServer) Shutdown() error {
	defer close(s.stopChan)
	return s.Stop()
}

// ServeHTTP handles all requests to Http API.
// ServeHTTP transforms HTTP request into a message.Request,
// sends it to MessageHandler, waits until message processed,
// receives message.Response, corresponding to sent Request
// and transorms message.Response into HTTP response
func (s *HttpServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		request  *message.Request
		response *message.Response
	)

	log.Debugf("Received request: %q", r.URL.EscapedPath())

	mr, err := r.MultipartReader()
	if err == http.ErrNotMultipart {
		request, err = parseSinglePayload(r)
	} else if err == nil {
		request, err = parseMultiPayload(r, mr)
	} else {
		log.Debugf("Error during processing request: %s", err.Error())
		http.Error(w, "Error during processing request: "+err.Error(), http.StatusBadRequest)
		return
	}

	if err != nil {
		log.Debugf("Error during processing request: %s", err.Error())
		http.Error(w, "Error during processing request: "+err.Error(), http.StatusBadRequest)
		return
	}

	log.Debugf("Handling request: %s", request)

	response = s.messageHandler.HandleMessage(request)

	log.Debugf("Sending response: %s", response)

	if len(response.MultiPayloads) > 0 {
		sendMultipartResponse(w, response)
	} else {
		httpStatus := getResponseHttpStatus(response)
		w.Header().Set(StatusHeader, response.Status.String())
		w.WriteHeader(httpStatus)
		w.Write(response.Payload)
	}
}

func sendMultipartResponse(w http.ResponseWriter, response *message.Response) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)
	httpStatus := getResponseHttpStatus(response)

	for _, val := range response.MultiPayloads {
		mh := make(textproto.MIMEHeader)
		mh.Set("Content-Type", "text/plain")
		partWriter, err := writer.CreatePart(mh)
		if err != nil {
			log.Debugf("Error writing multipart response: %s", err.Error())
			http.Error(w, "Error during processing request: "+err.Error(), http.StatusInternalServerError)
			return
		}

		_, err = partWriter.Write(val)
		if err != nil {
			log.Debugf("Error writing multipart response: %s", err.Error())
			http.Error(w, "Error during processing request: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	err := writer.Close()
	if err != nil {
		log.Debugf("Error writing multipart response: %s", err.Error())
		http.Error(w, "Error during processing request: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", writer.FormDataContentType())
	w.Header().Set(StatusHeader, response.Status.String())
	w.WriteHeader(httpStatus)
	io.Copy(w, body)
}

func getResponseHttpStatus(r *message.Response) int {
	statusMap := map[message.Status]int{
		message.StatusOk:               http.StatusOK,
		message.StatusNotFound:         http.StatusNotFound,
		message.StatusError:            http.StatusInternalServerError,
		message.StatusInvalidCommand:   http.StatusBadRequest,
		message.StatusTypeMismatch:     http.StatusBadRequest,
		message.StatusInvalidArguments: http.StatusBadRequest,
	}

	if httpStatus, ok := statusMap[r.Status]; ok {
		return httpStatus
	} else {
		return http.StatusInternalServerError
	}
}

func getCmdArgs(r *http.Request) (cmd string, args []string, err error) {
	urlParts := strings.Split(r.URL.EscapedPath(), "/")
	if len(urlParts) < 3 {
		return "", nil, errors.New("min URL parts count is 3")
	}

	cmd, err = url.PathUnescape(urlParts[1])
	if err != nil {
		return "", nil, err
	}

	args = make([]string, len(urlParts[2:]))
	for i, v := range urlParts[2:] {
		if args[i], err = url.PathUnescape(v); err != nil {
			return "", nil, err
		}
	}

	return cmd, args, nil
}

// parseMultiPayload parses regular one-part http request and returns message.Request
func parseSinglePayload(r *http.Request) (req *message.Request, err error) {
	cmd, args, err := getCmdArgs(r)
	if err != nil {
		return nil, err
	}
	payload, err := ioutil.ReadAll(r.Body)

	return message.NewRequestSingle(cmd, args, payload), err
}

// parseMultiPayload parses multipart http request and returns message.Request
func parseMultiPayload(r *http.Request, mr *multipart.Reader) (req *message.Request, err error) {
	cmd, args, err := getCmdArgs(r)
	if err != nil {
		return nil, err
	}

	var multiPayload [][]byte
	for p, err := mr.NextPart(); err == nil; p, err = mr.NextPart() {
		payload, err := ioutil.ReadAll(p)

		if err != nil {
			log.Debugf("Error reading part: %s...", err.Error())
			return nil, err
		}

		multiPayload = append(multiPayload, payload)
	}

	return message.NewRequestMulti(cmd, args, multiPayload), nil
}
