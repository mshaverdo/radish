package restless

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/mshaverdo/radish/api"
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

// Server is a implementation of Server interface
type Server struct {
	http.Server
	messageHandler api.MessageHandler
	stopChan       chan struct{}
}

// NewServer Returns new instance of Radish HTTP server
func NewServer(host string, port int, messageHandler api.MessageHandler) *Server {
	// use server instance instead of http.ListenAndServe -- due to we should use graceful shutdown
	addr := fmt.Sprintf("%s:%d", host, port)

	s := Server{
		Server:         http.Server{Addr: addr},
		messageHandler: messageHandler,
		stopChan:       make(chan struct{}),
	}

	s.Server.Handler = &s

	return &s
}

// ListenAndServe statrs listening to incoming connections
func (s *Server) ListenAndServe() error {
	if err := s.Server.ListenAndServe(); err == http.ErrServerClosed {
		<-s.stopChan // wait for full shutdown
		return nil
	} else {
		return err
	}
}

// Stops accepting new requests by HTTP server, but not causes return from ListenAndServe() until Shutdown()
func (s *Server) Stop() error {
	return s.Server.Shutdown(context.TODO())
}

// Shutdown gracefully shuts server down
func (s *Server) Shutdown() error {
	defer close(s.stopChan)
	return s.Stop()
}

// ServeHTTP handles all requests to Http API.
// ServeHTTP transforms HTTP request into a message.Request,
// sends it to MessageHandler, waits until message processed,
// receives message.Response, corresponding to sent Request
// and transorms message.Response into HTTP response
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		request  *message.Request
		response message.Response
	)

	log.Debugf("Received request: %q", r.URL.EscapedPath())

	request, err := parseRequest(r)
	if err != nil {
		log.Debugf("Error during processing request: %s", err.Error())
		http.Error(w, "Error during processing request: "+err.Error(), http.StatusBadRequest)
		return
	}

	log.Debugf("Handling request: %s", request)

	response = s.messageHandler.HandleMessage(request)

	log.Debugf("Sending response: %s", response)

	sendResponse(response, w)
}

func sendResponse(response message.Response, w http.ResponseWriter) {
	var (
		bodyReader io.Reader
		err        error
	)

	if len(response.Bytes()) > 1 {
		var contentType string
		bodyReader, contentType, err = assembleMultipartResponse(response)
		w.Header().Set("Content-Type", contentType)
	} else if len(response.Bytes()) == 1 {
		bodyReader = bytes.NewReader(response.Bytes()[0])
	} else {
		bodyReader = bytes.NewReader(nil)
	}

	if err != nil {
		log.Debugf("Error writing multipart response: %s", err.Error())
		http.Error(w, "Error during processing request: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set(StatusHeader, response.Status().String())
	w.WriteHeader(getResponseHttpStatus(response))
	io.Copy(w, bodyReader)
}

func assembleMultipartResponse(response message.Response) (bodyReader io.Reader, contentType string, err error) {
	bodyBuffer := &bytes.Buffer{}
	writer := multipart.NewWriter(bodyBuffer)

	for _, val := range response.Bytes() {
		mh := make(textproto.MIMEHeader)
		mh.Set("Content-Type", "text/plain")
		partWriter, err := writer.CreatePart(mh)
		if err != nil {
			return nil, "", err
		}

		if _, err = partWriter.Write(val); err != nil {
			return nil, "", err
		}
	}
	if err = writer.Close(); err != nil {
		return nil, "", err
	}

	contentType = writer.FormDataContentType()
	return bodyBuffer, contentType, nil
}

func getResponseHttpStatus(r message.Response) int {
	statusMap := map[message.Status]int{
		message.StatusOk:               http.StatusOK,
		message.StatusNotFound:         http.StatusNotFound,
		message.StatusError:            http.StatusInternalServerError,
		message.StatusInvalidCommand:   http.StatusBadRequest,
		message.StatusTypeMismatch:     http.StatusBadRequest,
		message.StatusInvalidArguments: http.StatusBadRequest,
	}

	if httpStatus, ok := statusMap[r.Status()]; ok {
		return httpStatus
	} else {
		return http.StatusInternalServerError
	}
}

func getCmdArgs(r *http.Request) (cmd string, args [][]byte, err error) {
	urlParts := strings.Split(r.URL.EscapedPath(), "/")
	if len(urlParts) < 3 {
		return "", nil, errors.New("min URL parts count is 3")
	}

	cmd, err = url.PathUnescape(urlParts[1])
	if err != nil {
		return "", nil, err
	}

	args = make([][]byte, len(urlParts[2:]))
	for i, v := range urlParts[2:] {
		arg, err := url.PathUnescape(v)
		if err != nil {
			return "", nil, err
		}
		args[i] = []byte(arg)
	}
	//fmt.Printf(">>>>>>>> %q\n", r.URL.EscapedPath())
	return cmd, args, nil
}

// parseRequest parses http request and returns message.Request
func parseRequest(httpRequest *http.Request) (*message.Request, error) {
	cmd, args, err := getCmdArgs(httpRequest)
	if err != nil {
		return nil, err
	}

	var payload [][]byte
	mr, err := httpRequest.MultipartReader()
	if err == nil {
		for p, err := mr.NextPart(); err == nil; p, err = mr.NextPart() {
			part, err := ioutil.ReadAll(p)
			if err != nil {
				return nil, err
			}

			payload = append(payload, part)
		}

	} else if err == http.ErrNotMultipart {
		payload = make([][]byte, 1)
		payload[0], err = ioutil.ReadAll(httpRequest.Body)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, err
	}

	if httpRequest.Method == "POST" {
		args = append(args, payload...)
	}

	return message.NewRequest(cmd, args), nil
}
