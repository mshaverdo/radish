package httpserver_test

import (
	"bytes"
	"errors"
	"github.com/go-test/deep"
	"github.com/mshaverdo/radish/controller/httpserver"
	"github.com/mshaverdo/radish/log"
	"github.com/mshaverdo/radish/message"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"
)

func init() {
	// set lowest log level to prevent test output pollution
	log.SetLevel(log.CRITICAL)
}

func TestHttpServer_SendResponse(t *testing.T) {
	var tests = []struct {
		response       message.Response
		wantHttpStatus int
	}{
		{
			message.NewResponseStatus(message.StatusOk, "共産主義の幽霊\n\"\r\n'\x00"),
			http.StatusOK,
		},
		{
			message.NewResponseStatus(message.StatusNotFound, "共産主義の幽霊\n\"\r\n'\x00"),
			http.StatusNotFound,
		},
		{
			message.NewResponseStatus(message.StatusError, "共産主義の幽霊\n\"\r\n'\x00"),
			http.StatusInternalServerError,
		},
		{
			message.NewResponseStatus(message.StatusInvalidCommand, "共産主義の幽霊\n\"\r\n'\x00"),
			http.StatusBadRequest,
		},
		{
			message.NewResponseInt(message.StatusOk, 42),
			http.StatusOK,
		},
		{
			message.NewResponseString(message.StatusOk, []byte("共産主義の幽霊\n\"\r\n'\x00")),
			http.StatusOK,
		},
		{
			message.NewResponseStringSlice(message.StatusOk, [][]byte{[]byte("共産主義の幽霊\n\"\r\n'\x00"), []byte("\x00")}),
			http.StatusOK,
		},
	}

	for n, tst := range tests {
		recorder := httptest.NewRecorder()
		httpserver.SendResponse(tst.response, recorder)

		if recorder.Code != tst.wantHttpStatus {
			t.Errorf("testcase %d: %q Invalid status code: got %d, want %d", n, tst.response.Status(), recorder.Code, tst.wantHttpStatus)
		}

		if recorder.Header().Get(httpserver.StatusHeader) != tst.response.Status().String() {
			t.Errorf(
				"testcase %d: Invalid radish status code: got %q, want %q",
				n,
				recorder.Header().Get(httpserver.StatusHeader),
				tst.response.Status().String(),
			)
		}

		if len(tst.response.Bytes()) > 1 {
			multiPayloads, err := praseMultipartResponse(recorder)
			if err != nil {
				t.Errorf("testcase %d: Unable to parse multipart response: %s", n, err.Error())
			}

			strPayloads := bytesSliceToStringsSlice(tst.response.Bytes())
			if diff := deep.Equal(multiPayloads, strPayloads); diff != nil {
				t.Errorf(
					"testcase %d: Invalid payload : %s\n\ngot: %q\n\nwant: %q",
					n,
					diff,
					multiPayloads,
					strPayloads,
				)
			}
		} else if len(tst.response.Bytes()) == 1 {
			if recorder.Body.String() != string(tst.response.Bytes()[0]) {
				t.Errorf("testcase %d: Invalid payload : %q != %q", n, recorder.Body.String(), tst.response.Bytes()[0])
			}
		}
	}
}

func TestHttpServer_ParseRequest(t *testing.T) {
	var tests = []struct {
		usePost       bool
		url           string
		payload       string
		multiPayloads []string
		wantCmd       string
		wantArgs      []string
		wantErr       error
	}{
		{
			true,
			"http://localhost:6380/CMD/OK1/%D1%84%D1%8B%2F%D0%B2%D0%B0%0A/%2A",
			"共産主義の幽霊\n\"\r\n'\x00",
			nil,
			"CMD",
			[]string{"OK1", "фы/ва\n", "*", "共産主義の幽霊\n\"\r\n'\x00"},
			nil,
		},
		{
			false,
			"http://localhost:6380/INVALID_SHORT_REQUEST",
			"",
			nil,
			"",
			nil,
			errors.New("min URL parts count is 3"),
		},
		{
			true,
			"http://localhost:6380/CMD/OK",
			"",
			[]string{"共産主義の幽霊\n\"\r\n'\x00", "", "\r\n\x00"},
			"CMD",
			[]string{"OK", "共産主義の幽霊\n\"\r\n'\x00", "", "\r\n\x00"},
			nil,
		},
		{
			false,
			"http://localhost:6380/CMD/OK",
			"",
			nil,
			"CMD",
			[]string{"OK", ""},
			nil,
		},
	}

	for _, tst := range tests {
		httpRequest := newMockRequest(tst.usePost, tst.url, tst.payload, tst.multiPayloads)
		request, err := httpserver.ParseRequest(httpRequest)

		if err != tst.wantErr && (err == nil || tst.wantErr == nil || err.Error() != tst.wantErr.Error()) {
			t.Errorf("%q : err got %q, want %q", tst.url, err, tst.wantErr)
		}

		if err != nil {
			//skip other checks if parsed with errors
			continue
		}

		if request.Cmd != tst.wantCmd {
			t.Errorf("%q CMD got: %q  want: %q", tst.url, request.Cmd, tst.wantCmd)
		}

		stringArgs := bytesSliceToStringsSlice(request.Args)
		if diff := deep.Equal(stringArgs, tst.wantArgs); diff != nil {
			t.Errorf(
				"%q Args differs from expected: %s \ngot: %s \nwant: %s",
				tst.url,
				diff,
				stringArgs,
				tst.wantArgs,
			)
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

func stringsSliceToBytesSlise(s []string) [][]byte {
	result := make([][]byte, len(s))
	for i, v := range s {
		result[i] = []byte(v)
	}

	return result
}

func bytesSliceToStringsSlice(b [][]byte) []string {
	result := make([]string, len(b))
	for i, v := range b {
		result[i] = string(v)
	}

	return result
}
