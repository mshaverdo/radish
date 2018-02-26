package radish

import (
	"bytes"
	"encoding"
	"errors"
	"fmt"
	"github.com/mshaverdo/assert"
	"io/ioutil"
	"mime"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"strconv"
)

func getRequestSingle(usePost bool, url string, payload []byte) (*http.Request, error) {
	method := map[bool]string{true: "POST", false: "GET"}[usePost]

	assert.False(!usePost && payload != nil, "POST must be used for non-nil payload requests")

	return http.NewRequest(method, url, bytes.NewReader(payload))
}

func getRequestMulti(url string, multiPayloads [][]byte) (*http.Request, error) {
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	for _, val := range multiPayloads {
		mh := make(textproto.MIMEHeader)
		mh.Set("Content-Type", "application/octet-stream")
		partWriter, err := writer.CreatePart(mh)
		if err != nil {
			return nil, err
		}
		_, err = partWriter.Write(val)
		if err != nil {
			return nil, err
		}
	}

	err := writer.Close()
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", writer.FormDataContentType())

	return req, nil
}

func parseResponseSingle(r *http.Response) (result []byte, err error) {
	return ioutil.ReadAll(r.Body)
}

func parseResponseMulti(r *http.Response) (result [][]byte, err error) {
	if r.Header.Get("Content-Length") == "0" {
		return nil, nil
	}

	v := r.Header.Get("Content-Type")
	d, params, err := mime.ParseMediaType(v)
	if err != nil || d != "multipart/form-data" {
		body, err := ioutil.ReadAll(r.Body)
		return [][]byte{body}, err
	}

	boundary, ok := params["boundary"]
	if !ok {
		return nil, errors.New("missing boundary")
	}

	reader := multipart.NewReader(r.Body, boundary)

	for p, err := reader.NextPart(); err == nil; p, err = reader.NextPart() {
		payload, err := ioutil.ReadAll(p)

		if err != nil {
			return nil, err
		}

		result = append(result, payload)
	}

	return result, nil
}

// inspired by go-redis WriteBuffer
func convertToBytes(val interface{}) ([]byte, error) {
	switch v := val.(type) {
	case nil:
		return []byte(nil), nil
	case string:
		return []byte(v), nil
	case []byte:
		return v, nil
	case int:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int8:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int16:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int32:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case int64:
		return []byte(strconv.FormatInt(int64(v), 10)), nil
	case uint:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint8:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint16:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint32:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case uint64:
		return []byte(strconv.FormatUint(uint64(v), 10)), nil
	case float32:
		return []byte(strconv.FormatFloat(float64(v), 'f', -1, 64)), nil
	case float64:
		return []byte(strconv.FormatFloat(float64(v), 'f', -1, 64)), nil
	case bool:
		if v {
			return []byte("1"), nil
		} else {
			return []byte("0"), nil
		}
	case encoding.BinaryMarshaler:
		bb, err := v.MarshalBinary()
		if err != nil {
			return nil, err
		}
		return bb, nil
	case fmt.Stringer:
		return []byte(v.String()), nil
	default:
		return nil, fmt.Errorf("radish: can't neither stringificate nor marshal %T (consider implementing encoding.BinaryMarshaler)", val)
	}
}
