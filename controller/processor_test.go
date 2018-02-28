package controller

import (
	"fmt"
	"github.com/mshaverdo/radish/message"
	"testing"
	"time"
)

func TestProcessor_FixRequestTtl(t *testing.T) {
	nowMinus5 := time.Now().Add(-5 * time.Second)

	tests := []struct {
		message  *message.Request
		wantArgs []string
	}{
		{
			&message.Request{
				Timestamp: nowMinus5.Unix(),
				Cmd:       "EXPIRE",
				Args:      [][]byte{[]byte("KEY"), []byte("15")},
			},
			[]string{"KEY", "10"},
		},
		{
			&message.Request{
				Timestamp: nowMinus5.Unix(),
				Cmd:       "SETEX",
				Args:      [][]byte{[]byte("KEY"), []byte("15"), []byte("DATA")},
			},
			[]string{"KEY", "10", "DATA"},
		},
		{
			&message.Request{
				Timestamp: nowMinus5.Unix(),
				Cmd:       "DEL",
				Args:      [][]byte{[]byte("KEY"), []byte("15"), []byte("DATA")},
			},
			[]string{"KEY", "15", "DATA"},
		},
	}

	for _, tst := range tests {
		p := NewProcessor(nil)
		p.FixRequestTtl(tst.message)

		got := fmt.Sprintf("%q", tst.message.Args)
		want := fmt.Sprintf("%q", tst.wantArgs)
		if got != want {
			t.Errorf("FixRequestTtl: %s != %s", got, want)
		}
	}
}
