package controller

import (
	"github.com/mshaverdo/radish/message"
	"strconv"
	"time"
)

//TODO: use go generate!
//TODO: move Cmd strings to constants!

type Processor struct {
	core Core
}

func NewProcessor(core Core) *Processor {
	return &Processor{core: core}
}

// Process processes request to Core
func (p *Processor) Process(request *message.Request) *message.Response {
	switch request.Cmd {
	case "KEYS":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result := p.core.Keys(arg0)

		return getResponseMultiStringPayload(result)
	case "GET":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result, err := p.core.Get(arg0)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseSinglePayload(result)
	case "SET":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		p.core.Set(arg0, request.Payload)

		return getResponseEmptyPayload()
	case "SETEX":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentInt(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		p.core.SetEx(arg0, arg1, request.Payload)

		return getResponseEmptyPayload()
	case "DEL":
		args, err := request.GetArgumentVariadicString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result := p.core.Del(args)

		return getResponseIntPayload(result)
	case "DKEYS":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentString(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result, err := p.core.DKeys(arg0, arg1)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseMultiStringPayload(result)
	case "DGETALL":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result, err := p.core.DGetAll(arg0)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseMultiPayload(result)

	case "DGET":
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

		return getResponseSinglePayload(result)
	case "DSET":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentString(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		count, err := p.core.DSet(arg0, arg1, request.Payload)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseIntPayload(count)
	case "DDEL":
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

		return getResponseMultiPayload(result)
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

		return getResponseSinglePayload(result)
	case "LSET":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentInt(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		err = p.core.LSet(arg0, arg1, request.Payload)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseEmptyPayload()
	case "LPUSH":
		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		count, err := p.core.LPush(arg0, request.MultiPayloads)
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

		return getResponseSinglePayload(result)
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
		return message.NewResponseSingle(
			message.StatusInvalidCommand,
			[]byte("Unknown command: "+request.Cmd),
		)
	}
}

// IsModifyingRequest returns true, if request modifies a storage
func (p *Processor) IsModifyingRequest(request *message.Request) bool {
	switch request.Cmd {
	case "SET", "SETEX", "DEL", "DSET", "DDEL", "LSET", "LPUSH", "LPOP", "EXPIRE", "PERSIST":
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
		request.Args[1] = strconv.Itoa(seconds)
	}

	return nil
}
