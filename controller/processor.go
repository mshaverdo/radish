package controller

import (
	"errors"
	"github.com/mshaverdo/radish/message"
	"strconv"
	"time"
)

//TODO: use go generate!

type Processor struct {
	core Core
}

func NewProcessor(core Core) *Processor {
	return &Processor{core: core}
}

// Process processes request to Core
func (p *Processor) Process(request *message.Request) message.Response {
	switch request.Cmd {
	case "KEYS":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result := p.core.Keys(arg0)

		return getResponseStringSlicePayload(stringsSliceToBytesSlise(result))
	case "GET":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result, err := p.core.Get(arg0)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseStringPayload(result)
	case "SET":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		arg1, err := request.GetArgumentBytes(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		//TODO: it's to avoid using SET with EX/PX arguments. If using GO Generate, just check exact count of args for all commands
		if request.ArgumentsLen() != 2 {
			return getResponseInvalidArguments(request.Cmd, errors.New("SET EX/PX not supported"))
		}

		p.core.Set(arg0, arg1)

		return getResponseStatusOkPayload()
	case "SETEX":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentInt(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg2, err := request.GetArgumentBytes(2)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		p.core.SetEx(arg0, arg1, arg2)

		return getResponseStatusOkPayload()
	case "DEL":
		args, err := request.GetArgumentVariadicString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result := p.core.Del(args)

		return getResponseIntPayload(result)
	case "HKEYS":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentString(1)
		if err != nil {
			//TODO: fixme in core
			// Oops. pattern isn't available in redis
			arg1 = "*"
		}
		result, err := p.core.DKeys(arg0, arg1)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseStringSlicePayload(stringsSliceToBytesSlise(result))
	case "HGETALL":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result, err := p.core.DGetAll(arg0)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseStringSlicePayload(result)

	case "HGET":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentString(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result, err := p.core.DGet(arg0, arg1)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseStringPayload(result)
	case "HSET":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentString(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg2, err := request.GetArgumentBytes(2)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		count, err := p.core.DSet(arg0, arg1, arg2)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseIntPayload(count)
	case "HDEL":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		args, err := request.GetArgumentVariadicString(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		count, err := p.core.DDel(arg0, args)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseIntPayload(count)
	case "LLEN":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		count, err := p.core.LLen(arg0)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseIntPayload(count)
	case "LRANGE":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentInt(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg2, err := request.GetArgumentInt(2)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result, err := p.core.LRange(arg0, arg1, arg2)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseStringSlicePayload(result)
	case "LINDEX":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentInt(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result, err := p.core.LIndex(arg0, arg1)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseStringPayload(result)
	case "LSET":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentInt(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg2, err := request.GetArgumentBytes(2)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		err = p.core.LSet(arg0, arg1, arg2)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseStatusOkPayload()
	case "LPUSH":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentVariadicBytes(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		count, err := p.core.LPush(arg0, arg1)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseIntPayload(count)
	case "LPOP":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result, err := p.core.LPop(arg0)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseStringPayload(result)
	case "TTL":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		ttl, err := p.core.Ttl(arg0)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseIntPayload(ttl)
	case "EXPIRE":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentInt(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result := p.core.Expire(arg0, arg1)

		return getResponseIntPayload(result)
	case "PERSIST":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result := p.core.Persist(arg0)

		return getResponseIntPayload(result)
	default:
		return message.NewResponseStatus(message.StatusInvalidCommand, "unknown command: "+request.Cmd)
	}
}

// IsModifyingRequest returns true, if request modifies a storage
func (p *Processor) IsModifyingRequest(request *message.Request) bool {
	switch request.Cmd {
	case "SET", "SETEX", "DEL", "HSET", "HDEL", "LSET", "LPUSH", "LPOP", "EXPIRE", "PERSIST":
		return true
	default:
		return false
	}
}

// FixWalRequestTtl Correct TTL value for TTL-related requests due to ttl is time.Now() -related value
func (p *Processor) FixRequestTtl(request *message.Request) error {
	switch request.Cmd {
	case "SETEX", "EXPIRE":
		seconds, err := request.GetArgumentInt(1)
		if err != nil {
			return err
		}

		seconds -= int(time.Since(request.Time).Seconds())
		request.Args[1] = []byte(strconv.Itoa(seconds))
	}

	return nil
}
