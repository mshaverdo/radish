package httpserver

import (
	"bytes"
	"errors"
	"github.com/go-test/deep"
	"github.com/mshaverdo/radish/log"
	"github.com/mshaverdo/radish/message"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"net/url"
	"testing"
	"time"
)

func init() {
	// set lowest log level to prevent test output pollution
	log.SetLevel(log.CRITICAL)
}

type MockMessageHandler struct {
	lastRequest           *message.Request
	responseSinglePayload string
	responseMultiPayloads []string
}

func newMockMessageHandler() *MockMessageHandler {
	return &MockMessageHandler{
		responseMultiPayloads: []string{"Payload1 Ы", "Payload2 Щ"},
		responseSinglePayload: "PAYLOAD Ф",
	}
}

// HandleMessage processes Request and return Response
func (m *MockMessageHandler) HandleMessage(request *message.Request) *message.Response {
	m.lastRequest = request

	status := message.StatusOk
	statuses := map[string]message.Status{
		"OK":        message.StatusOk,
		"ERROR":     message.StatusError,
		"WRONGTYPE": message.StatusTypeMismatch,
		"NOTFOUND":  message.StatusNotFound,
	}
	if len(request.Args) > 0 {
		status = statuses[request.Args[0]]
	}

	if len(request.MultiPayloads) > 0 {
		multiPayloads := [][]byte{}
		for _, v := range m.responseMultiPayloads {
			multiPayloads = append(multiPayloads, []byte(v))
		}
		return message.NewResponseMulti(status, multiPayloads)
	} else {
		return message.NewResponseSingle(status, []byte(m.responseSinglePayload))
	}
}

func TestHttpServer_ServeHTTP(t *testing.T) {
	var tests = []struct {
		usePost          bool
		url              string
		payload          string
		multiPayloads    []string
		wantMessage      *message.Request
		wantHttpStatus   int
		wantRadishStatus message.Status
	}{
		{
			true,
			"http://localhost:6380/DEL/OK1/%D1%84%D1%8B%2F%D0%B2%D0%B0%0A", //"http://localhost:6380/DEL/OK1/" + url.PathEscape("фы/ва\n"),
			"",
			nil,
			message.NewRequestSingle("DEL", []string{"OK1", "фы/ва\n"}, []byte("")),
			http.StatusOK,
			message.StatusOk,
		},
		{
			false,
			"http://localhost:6380/INVALID_SHORT_REQUEST",
			"",
			nil,
			nil,
			http.StatusBadRequest,
			message.StatusOk,
		},
		{
			false,
			"http://localhost:6380/KEYS/" + url.PathEscape("*"),
			"",
			nil,
			message.NewRequestSingle("KEYS", []string{"*"}, []byte("")),
			http.StatusOK,
			message.StatusOk,
		},
		{
			false,
			"http://localhost:6380/GET/NOTFOUND",
			"",
			nil,
			message.NewRequestSingle("GET", []string{"NOTFOUND"}, []byte("")),
			http.StatusNotFound,
			message.StatusNotFound,
		},
		{
			true,
			"http://localhost:6380/LPUSH/OK",
			"",
			[]string{"val1", "ЫФ3\n\"\r"},
			message.NewRequestMulti("LPUSH", []string{"OK"}, [][]byte{[]byte("val1"), []byte("ЫФ3\n\"\r")}),
			http.StatusOK,
			message.StatusOk,
		},
		{
			true,
			"http://localhost:6380/LPUSH/WRONGTYPE",
			"",
			nil,
			message.NewRequestSingle("LPUSH", []string{"WRONGTYPE"}, []byte("")),
			http.StatusBadRequest,
			message.StatusTypeMismatch,
		},
	}

	for _, test := range tests {
		mockHandler := newMockMessageHandler()
		s := New("", 0, mockHandler)
		recorder := httptest.NewRecorder()
		req := newMockRequest(test.usePost, test.url, test.payload, test.multiPayloads)
		s.ServeHTTP(recorder, req)

		// clear request times to avoid nanosecond differences
		if test.wantMessage != nil {
			test.wantMessage.Time = time.Time{}
			mockHandler.lastRequest.Time = time.Time{}
		}

		if recorder.Code != test.wantHttpStatus {
			t.Errorf("%q Invalid status code: got %d, want %d", test.url, recorder.Code, test.wantHttpStatus)
		}

		if diff := deep.Equal(mockHandler.lastRequest, test.wantMessage); diff != nil {
			t.Errorf(
				"%q Received message differs from expected: %s \ngot: %s \nwant: %s",
				test.url,
				diff,
				mockHandler.lastRequest,
				test.wantMessage,
			)
		}

		if test.wantMessage == nil {
			// SKIP any further checks due to HandleMessage() wasn't invoked
			continue
		}

		if recorder.Header().Get(StatusHeader) != test.wantRadishStatus.String() {
			t.Errorf(
				"%q Invalid radish status code: got %q, want %q",
				test.url,
				recorder.Header().Get(StatusHeader),
				test.wantRadishStatus.String(),
			)
		}

		if len(test.multiPayloads) > 0 {
			multiPayloads, err := praseMultipartResponse(recorder)
			if err != nil {
				t.Errorf("%q Unable to parse multipart response: %s", test.url, err.Error())
			}

			if diff := deep.Equal(multiPayloads, mockHandler.responseMultiPayloads); diff != nil {
				t.Errorf(
					"%q Invalid payload : %s\n\ngot: %q\n\nwant: %q",
					test.url,
					diff,
					multiPayloads,
					mockHandler.responseMultiPayloads,
				)
			}
		} else {
			if recorder.Body.String() != mockHandler.responseSinglePayload {
				t.Errorf(
					"%q Invalid payload : %q != %q",
					test.url,
					recorder.Body.String(),
					mockHandler.responseSinglePayload,
				)
			}
		}
	}
}

func newMockRequest(usePost bool, url string, payload string, multiPayloads []string) (req *http.Request) {
	method := map[bool]string{true: "POST", false: "GET"}[usePost]

	if len(multiPayloads) > 0 {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for _, val := range multiPayloads {
			mh := make(textproto.MIMEHeader)
			mh.Set("Content-Type", "application/octet-stream")
			partWriter, _ := writer.CreatePart(mh)
			partWriter.Write([]byte(val))
		}

		writer.Close()

		req = httptest.NewRequest(method, url, body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
	} else {
		req = httptest.NewRequest(method, url, bytes.NewReader([]byte(payload)))
	}

	return req
}

func praseMultipartResponse(r *httptest.ResponseRecorder) (result []string, err error) {
	v := r.Header().Get("Content-Type")
	if v == "" {
		return nil, errors.New("Not a multipart")
	}
	d, params, err := mime.ParseMediaType(v)
	if err != nil || d != "multipart/form-data" {
		return nil, errors.New("Not a multipart")
	}
	boundary, ok := params["boundary"]
	if !ok {
		return nil, errors.New("Missing boundary")
	}

	reader := multipart.NewReader(r.Body, boundary)

	result = []string{}
	for p, err := reader.NextPart(); err == nil; p, err = reader.NextPart() {
		payload, err := ioutil.ReadAll(p)

		if err != nil {
			return nil, err
		}

		result = append(result, string(payload))
	}
	return result, nil
}
