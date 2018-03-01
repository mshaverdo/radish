package controller

import (
	"bufio"
	"encoding/binary"
	"github.com/go-test/deep"
	"github.com/mshaverdo/radish/message"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func BenchmarkGencodeEncoder_Encode(b *testing.B) {
	file, err := ioutil.TempFile("", "gencode_encoder")
	w := bufio.NewWriter(file)

	if err != nil {
		b.Fatalf("Failed to create temp file: %s", err)
	}

	defer func() {
		name := file.Name()
		file.Close()
		os.Remove(name)
	}()

	encoder := NewGencodeEncoder(w)
	request := message.NewRequest("SET", [][]byte{[]byte("000000000001"), []byte("XXX")})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder.Encode(request)
	}
	w.Flush()
	b.StopTimer()
}

func BenchmarkGencodeEncoder_Decode(b *testing.B) {
	file, err := ioutil.TempFile("", "gencode_encoder")
	w := bufio.NewWriter(file)

	if err != nil {
		b.Fatalf("Failed to create temp file: %s", err)
	}

	defer func() {
		name := file.Name()
		file.Close()
		os.Remove(name)
	}()

	encoder := NewGencodeEncoder(w)
	request := message.NewRequest("SET", [][]byte{[]byte("000000000001"), []byte("XXX")})

	for i := 0; i < b.N; i++ {
		request.Id = int64(i)
		encoder.Encode(request)
	}
	w.Flush()

	file.Seek(0, 0)
	decoder := NewGencodeDecoder(file)

	b.ResetTimer()
	request = new(message.Request)
	for i := 0; i < b.N; i++ {
		decoder.Decode(request)
	}

	b.StopTimer()
}

func TestGencodeEncoder_EncodeDecode(t *testing.T) {
	file, err := ioutil.TempFile("", "gencode_encoder")
	w := bufio.NewWriter(file)

	if err != nil {
		t.Errorf("Failed to create temp file: %s", err)
	}

	defer func() {
		name := file.Name()
		file.Close()
		os.Remove(name)
	}()

	encoder := NewGencodeEncoder(w)
	srcRequests := make([]*message.Request, 100)
	for i := 0; i < len(srcRequests); i++ {
		srcRequests[i] = message.NewRequest("SET", [][]byte{[]byte("000000000001"), []byte("XXX")})
		srcRequests[i].Id = int64(i)
		encoder.Encode(srcRequests[i])
	}
	// write extra data to end of file to check broken entries skip
	binary.Write(w, binary.LittleEndian, uint64(32))
	binary.Write(w, binary.LittleEndian, uint64(0))
	w.Flush()

	file.Seek(0, 0)
	decoder := NewGencodeDecoder(file)

	requests := make([]*message.Request, 0)
	request := new(message.Request)
	for err = decoder.Decode(request); err != io.EOF; err = decoder.Decode(request) {
		if err != nil {
			t.Errorf("failed to read file: %s", err)
		}
		requests = append(requests, request)
		request = new(message.Request)
	}

	if diff := deep.Equal(requests, srcRequests); diff != nil {
		t.Errorf("requests != srcRequests: %s", diff)
	}
}
