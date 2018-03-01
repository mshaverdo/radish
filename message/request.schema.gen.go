package message

import (
	"io"
	"time"
	"unsafe"
)

var (
	_ = unsafe.Sizeof(0)
	_ = io.ReadFull
	_ = time.Now()
)

type Request struct {
	Timestamp  int64
	Id         int64
	Cmd        string
	Args       [][]byte
	Unreliable bool
}

func (d *Request) Size() (s uint64) {

	{
		l := uint64(len(d.Cmd))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}
		s += l
	}
	{
		l := uint64(len(d.Args))

		{

			t := l
			for t >= 0x80 {
				t >>= 7
				s++
			}
			s++

		}

		for k0 := range d.Args {

			{
				l := uint64(len(d.Args[k0]))

				{

					t := l
					for t >= 0x80 {
						t >>= 7
						s++
					}
					s++

				}
				s += l
			}

		}

	}
	s += 17
	return
}
func (d *Request) Marshal(buf []byte) ([]byte, error) {
	size := d.Size()
	{
		if uint64(cap(buf)) >= size {
			buf = buf[:size]
		} else {
			buf = make([]byte, size)
		}
	}
	i := uint64(0)

	{

		buf[0+0] = byte(d.Timestamp >> 0)

		buf[1+0] = byte(d.Timestamp >> 8)

		buf[2+0] = byte(d.Timestamp >> 16)

		buf[3+0] = byte(d.Timestamp >> 24)

		buf[4+0] = byte(d.Timestamp >> 32)

		buf[5+0] = byte(d.Timestamp >> 40)

		buf[6+0] = byte(d.Timestamp >> 48)

		buf[7+0] = byte(d.Timestamp >> 56)

	}
	{

		buf[0+8] = byte(d.Id >> 0)

		buf[1+8] = byte(d.Id >> 8)

		buf[2+8] = byte(d.Id >> 16)

		buf[3+8] = byte(d.Id >> 24)

		buf[4+8] = byte(d.Id >> 32)

		buf[5+8] = byte(d.Id >> 40)

		buf[6+8] = byte(d.Id >> 48)

		buf[7+8] = byte(d.Id >> 56)

	}
	{
		l := uint64(len(d.Cmd))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+16] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+16] = byte(t)
			i++

		}
		copy(buf[i+16:], d.Cmd)
		i += l
	}
	{
		l := uint64(len(d.Args))

		{

			t := uint64(l)

			for t >= 0x80 {
				buf[i+16] = byte(t) | 0x80
				t >>= 7
				i++
			}
			buf[i+16] = byte(t)
			i++

		}
		for k0 := range d.Args {

			{
				l := uint64(len(d.Args[k0]))

				{

					t := uint64(l)

					for t >= 0x80 {
						buf[i+16] = byte(t) | 0x80
						t >>= 7
						i++
					}
					buf[i+16] = byte(t)
					i++

				}
				copy(buf[i+16:], d.Args[k0])
				i += l
			}

		}
	}
	{
		if d.Unreliable {
			buf[i+16] = 1
		} else {
			buf[i+16] = 0
		}
	}
	return buf[:i+17], nil
}

func (d *Request) Unmarshal(buf []byte) (uint64, error) {
	i := uint64(0)

	{

		d.Timestamp = 0 | (int64(buf[i+0+0]) << 0) | (int64(buf[i+1+0]) << 8) | (int64(buf[i+2+0]) << 16) | (int64(buf[i+3+0]) << 24) | (int64(buf[i+4+0]) << 32) | (int64(buf[i+5+0]) << 40) | (int64(buf[i+6+0]) << 48) | (int64(buf[i+7+0]) << 56)

	}
	{

		d.Id = 0 | (int64(buf[i+0+8]) << 0) | (int64(buf[i+1+8]) << 8) | (int64(buf[i+2+8]) << 16) | (int64(buf[i+3+8]) << 24) | (int64(buf[i+4+8]) << 32) | (int64(buf[i+5+8]) << 40) | (int64(buf[i+6+8]) << 48) | (int64(buf[i+7+8]) << 56)

	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+16] & 0x7F)
			for buf[i+16]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+16]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		d.Cmd = string(buf[i+16 : i+16+l])
		i += l
	}
	{
		l := uint64(0)

		{

			bs := uint8(7)
			t := uint64(buf[i+16] & 0x7F)
			for buf[i+16]&0x80 == 0x80 {
				i++
				t |= uint64(buf[i+16]&0x7F) << bs
				bs += 7
			}
			i++

			l = t

		}
		if uint64(cap(d.Args)) >= l {
			d.Args = d.Args[:l]
		} else {
			d.Args = make([][]byte, l)
		}
		for k0 := range d.Args {

			{
				l := uint64(0)

				{

					bs := uint8(7)
					t := uint64(buf[i+16] & 0x7F)
					for buf[i+16]&0x80 == 0x80 {
						i++
						t |= uint64(buf[i+16]&0x7F) << bs
						bs += 7
					}
					i++

					l = t

				}
				if uint64(cap(d.Args[k0])) >= l {
					d.Args[k0] = d.Args[k0][:l]
				} else {
					d.Args[k0] = make([]byte, l)
				}
				copy(d.Args[k0], buf[i+16:])
				i += l
			}

		}
	}
	{
		d.Unreliable = buf[i+16] == 1
	}
	return i + 17, nil
}
