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
	"testing"
)

type MockMessageHandler struct {
	lastRequest           *message.Request
	responseSinglePayload string
	responseMultiPayloads map[string]string
}

// HandleMessage processes Request and return Response
func (m *MockMessageHandler) HandleMessage(request *message.Request) *message.Response {
	m.responseSinglePayload = "PAYLOAD Ф"
	m.responseMultiPayloads = map[string]string{"1": "Payload1 Ы", "2": "Payload2 Щ"}

	m.lastRequest = request

	status := message.StatusOk
	statuses := map[string]message.Status{"OK": message.StatusOk, "ERROR": message.StatusError, "NOTFOUND": message.StatusNotFound}
	if len(request.Args) > 0 {
		status = statuses[request.Args[0]]
	}

	if len(request.MultiPayloads) > 0 {
		multiPayloads := map[string][]byte{}
		for k, v := range m.responseMultiPayloads {
			multiPayloads[k] = []byte(v)
		}
		return message.NewResponse(status, nil, multiPayloads)
	} else {
		return message.NewResponse(status, []byte(m.responseSinglePayload), nil)
	}
}

func TestHttpServer_ServeHTTP(t *testing.T) {
	var testsSingle = []struct {
		usePost     bool
		uri         string
		payload     string
		meta        map[string]string
		wantMessage *message.Request
		wantStatus  int
	}{
		{
			false,
			"http://localhost:8380/KEYS",
			"",
			nil,
			message.NewRequest("KEYS", []string{}, map[string]string{}, []byte(""), nil),
			200,
		},
		{
			false,
			"http://localhost:8380/GET/NOTFOUND",
			"",
			map[string]string{"X-Radish-Ttl": "100"},
			message.NewRequest("GET", []string{"NOTFOUND"}, map[string]string{"Ttl": "100"}, []byte(""), nil),
			404,
		},
	}

	var testsMulti = []struct {
		uri           string
		multiPayloads map[string]string
		meta          map[string]string
		wantMessage   *message.Request
		wantStatus    int
	}{
		{
			"http://localhost:8380/MSET/OK",
			map[string]string{"param1": "val1", "param2": "ЫФ3\n\"\r"},
			nil,
			message.NewRequest(
				"MSET",
				[]string{"OK"},
				map[string]string{},
				nil,
				map[string][]byte{"param1": []byte("val1"), "param2": []byte("ЫФ3\n\"\r")},
			),
			200,
		},
	}

	for _, test := range testsSingle {
		mockHandler := new(MockMessageHandler)
		s := New("", 0, mockHandler)
		recorder := httptest.NewRecorder()
		req := newMockSingleRequest(test.usePost, test.uri, test.payload, test.meta)
		s.ServeHTTP(recorder, req)

		if diff := deep.Equal(mockHandler.lastRequest, test.wantMessage); diff != nil {
			t.Errorf("Received message differs from expected: %s", diff)
		}

		if recorder.Code != test.wantStatus {
			t.Errorf("Invalid status code: got %d, want %d", recorder.Code, test.wantStatus)
		}

		if diff := deep.Equal(recorder.Body.String(), mockHandler.responseSinglePayload); diff != nil {
			t.Errorf("Invalid payload : %s", diff)
		}
	}

	for _, test := range testsMulti {
		mockHandler := new(MockMessageHandler)
		s := New("", 0, mockHandler)
		recorder := httptest.NewRecorder()
		req := newMockMultiRequest(test.uri, test.multiPayloads, test.meta)
		s.ServeHTTP(recorder, req)

		if diff := deep.Equal(mockHandler.lastRequest, test.wantMessage); diff != nil {
			t.Errorf("Received message differs from expected: %s", diff)
		}

		if recorder.Code != test.wantStatus {
			t.Errorf("Invalid status code: got %d, want %d", recorder.Code, test.wantStatus)
		}

		multipayloads, err := praseMultipartResponse(recorder)
		if err != nil {
			t.Errorf("Unable to parse multipart response: %s", err.Error())
			t.Logf("Recorded data: %s", recorder.Body.String())
		}

		if diff := deep.Equal(multipayloads, mockHandler.responseMultiPayloads); diff != nil {
			t.Errorf("Invalid payload : %s", diff)
		}
	}
}

func newMockSingleRequest(usePost bool, uri string, payload string, meta map[string]string) *http.Request {
	method := map[bool]string{true: "POST", false: "GET"}[usePost]

	req := httptest.NewRequest(method, uri, bytes.NewReader([]byte(payload)))

	for i, v := range meta {
		req.Header.Set(i, v)
	}

	return req
}

func newMockMultiRequest(uri string, params map[string]string, meta map[string]string) *http.Request {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	for key, val := range params {
		_ = writer.WriteField(key, val)
	}
	writer.Close()

	req := httptest.NewRequest("POST", uri, body)
	req.Header.Set("Content-Type", writer.FormDataContentType())
	for i, v := range meta {
		req.Header.Set(i, v)
	}

	return req
}

func praseMultipartResponse(r *httptest.ResponseRecorder) (result map[string]string, err error) {
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

	result = map[string]string{}
	for p, err := reader.NextPart(); err == nil; p, err = reader.NextPart() {
		payload, err := ioutil.ReadAll(p)

		if err != nil {
			return nil, err
		}

		result[p.FormName()] = string(payload)
	}
	return result, nil
}
