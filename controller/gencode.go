package controller

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/mshaverdo/assert"
	"io"
)

type Marshaller interface {
	Marshal(buf []byte) ([]byte, error)
}

type Unmarshaller interface {
	Unmarshal(buf []byte) (uint64, error)
}

type GencodeEncoder struct {
	writer io.Writer
	buf    []byte
}

func NewGencodeEncoder(writer io.Writer) *GencodeEncoder {
	return &GencodeEncoder{writer: writer}
}

func (ge *GencodeEncoder) Encode(val Marshaller) error {
	var err error
	ge.buf, err = val.Marshal(ge.buf)
	if err != nil {
		return err
	}

	err = binary.Write(ge.writer, binary.LittleEndian, uint64(len(ge.buf))) //it will write exactly 8 bytes
	if err != nil {
		return err
	}

	n, err := ge.writer.Write(ge.buf)
	if err != nil {
		return err
	}
	if n != len(ge.buf) {
		return fmt.Errorf("gocode encoding failed: only %d of %d bytes written", n, len(ge.buf))
	}

	return nil
}

type GencodeDecoder struct {
	reader io.Reader
}

func NewGencodeDecoder(reader io.Reader) *GencodeDecoder {
	return &GencodeDecoder{reader: bufio.NewReader(reader)}
}

func (gd *GencodeDecoder) Decode(val Unmarshaller) error {
	var sizeUint64 uint64
	err := binary.Read(gd.reader, binary.LittleEndian, &sizeUint64)
	if err != nil {
		if err == io.ErrUnexpectedEOF {
			err = io.EOF
		}
		return err
	}
	size := int(sizeUint64)

	buf := make([]byte, size)
	read := 0
	for read < size {
		n, err := gd.reader.Read(buf[read:])
		read += n
		if err == bufio.ErrBufferFull {
			continue
		}
		if err != nil {
			return err
		}
	}
	assert.True(read == size, "Can't read full blob from buffer!")

	_, err = val.Unmarshal(buf)
	if err != nil {
		return err
	}

	return nil
}
