package httpserver

import (
	"bytes"
	"errors"
	"github.com/go-test/deep"
	"github.com/mshaverdo/radish/message"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"
)

//TODO: Refactor this test
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
		"OK":       message.StatusOk,
		"ERROR":    message.StatusError,
		"NOTFOUND": message.StatusNotFound,
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
		usePost       bool
		uri           string
		payload       string
		multiPayloads []string
		wantMessage   *message.Request
		wantStatus    int
	}{
		{
			false,
			"http://localhost:8380/KEYS",
			"",
			nil,
			message.NewRequestSingle("KEYS", []string{}, map[string]string{}, []byte("")),
			http.StatusOK,
		},
		{
			false,
			"http://localhost:8380/GET/NOTFOUND",
			"",
			nil,
			message.NewRequestSingle("GET", []string{"NOTFOUND"}, map[string]string{}, []byte("")),
			http.StatusNotFound,
		},
		{
			true,
			"http://localhost:8380/DEL/OK1/OK2",
			"",
			nil,
			message.NewRequestSingle("DEL", []string{"OK1", "OK2"}, map[string]string{}, []byte("")),
			http.StatusOK,
		},
		{
			true,
			"http://localhost:8380/LPUSH/OK",
			"",
			[]string{"val1", "ЫФ3\n\"\r"},
			message.NewRequestMulti("LPUSH", []string{"OK"}, map[string]string{}, [][]byte{[]byte("val1"), []byte("ЫФ3\n\"\r")}),
			http.StatusOK,
		},
	}

	for _, test := range tests {
		mockHandler := newMockMessageHandler()
		s := New("", 0, mockHandler)
		recorder := httptest.NewRecorder()
		req := newMockRequest(test.usePost, test.uri, test.payload, test.multiPayloads)
		s.ServeHTTP(recorder, req)

		if diff := deep.Equal(mockHandler.lastRequest, test.wantMessage); diff != nil {
			t.Errorf("Received message differs from expected: %s", diff)
		}

		if recorder.Code != test.wantStatus {
			t.Errorf("%q Invalid status code: got %d, want %d", test.uri, recorder.Code, test.wantStatus)
		}

		if len(test.multiPayloads) > 0 {
			multiPayloads, err := praseMultipartResponse(recorder)
			if err != nil {
				t.Errorf("%q Unable to parse multipart response: %s", test.uri, err.Error())
			}

			if diff := deep.Equal(multiPayloads, mockHandler.responseMultiPayloads); diff != nil {
				t.Errorf("%q Invalid payload : %s\n\ngot: %q\n\nwant: %q", test.uri, diff, multiPayloads, mockHandler.responseMultiPayloads)
			}
		} else {
			if diff := deep.Equal(recorder.Body.String(), mockHandler.responseSinglePayload); diff != nil {
				t.Errorf("%q Invalid payload : %s", test.uri, diff)
			}
		}
	}
}

func newMockRequest(usePost bool, uri string, payload string, multiPayloads []string) (req *http.Request) {
	method := map[bool]string{true: "POST", false: "GET"}[usePost]

	if len(multiPayloads) > 0 {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)

		for _, val := range multiPayloads {
			mh := make(textproto.MIMEHeader)
			mh.Set("Content-Type", "text/plain")
			partWriter, _ := writer.CreatePart(mh)
			partWriter.Write([]byte(val))
		}

		writer.Close()

		req = httptest.NewRequest(method, uri, body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
	} else {
		req = httptest.NewRequest(method, uri, bytes.NewReader([]byte(payload)))
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
