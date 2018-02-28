package controller

//TODO: implement tests & benchmarks
import (
	"bytes"
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
	buf    *bytes.Buffer // intermediate buffer gives x5 performance boost
}

func NewGencodeDecoder(reader io.Reader) *GencodeDecoder {
	return &GencodeDecoder{reader: reader, buf: bytes.NewBuffer(nil)}
}

func (gd *GencodeDecoder) Decode(val Unmarshaller) error {
	uint64Size := 8

	if gd.buf.Len() < uint64Size {
		_, err := io.CopyN(gd.buf, gd.reader, walBufferSize)
		if err == io.EOF && gd.buf.Len() < uint64Size {
			// Both all ok, file finished or we have fragment of uint64 in buffer and reached EOF:
			// seems like a power failure during writing request len. But it's OK too:
			// we just skip last broken record (credits to Redis for idea)
			return io.EOF
		}
		if err != nil && err != io.EOF {
			return err
		}
	}

	var sizeUint64 uint64
	binary.Read(gd.buf, binary.LittleEndian, &sizeUint64)
	size := int(sizeUint64)

	for gd.buf.Len() < size {
		_, err := io.CopyN(gd.buf, gd.reader, walBufferSize)
		if err == io.EOF && gd.buf.Len() < size {
			// Both all ok, file finished or we have fragment of uint64 in buffer and reached EOF:
			// seems like a power failure during writing request len. But it's OK too:
			// we just skip last broken record (credits to Redis for idea)
			return io.EOF
		}
		if err != nil && err != io.EOF {
			return err
		}
	}

	buf := make([]byte, size)
	n, _ := gd.buf.Read(buf)
	assert.True(n == size, "Can't read full blob from buffer!")

	_, err := val.Unmarshal(buf)
	if err != nil {
		return err
	}

	return nil
}
