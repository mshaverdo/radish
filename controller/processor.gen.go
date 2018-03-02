/*
 * CODE GENERATED AUTOMATICALLY WITH github.com/mshaverdo/radish/codegen/processor
 * THIS FILE SHOULD NOT BE EDITED BY HAND!
 */

package controller

import (
	"fmt"
	"github.com/mshaverdo/radish/message"
	"strconv"
	"time"
)

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
		if request.ArgumentsLen() != 1 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result := p.core.Keys(arg0)

		return getResponseStringSlicePayload(stringsSliceToBytesSlise(result))
	case "GET":
		if request.ArgumentsLen() != 1 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

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
		if request.ArgumentsLen() != 2 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentBytes(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		p.core.Set(arg0, arg1)

		return getResponseStatusOkPayload()
	case "SETEX":
		if request.ArgumentsLen() != 3 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

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

		arg0, err := request.GetArgumentVariadicString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result := p.core.Del(arg0)

		return getResponseIntPayload(result)
	case "HSET":
		if request.ArgumentsLen() != 3 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

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

		result, err := p.core.DSet(arg0, arg1, arg2)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseIntPayload(result)
	case "HGET":
		if request.ArgumentsLen() != 2 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

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
	case "HKEYS":
		if request.ArgumentsLen() != 1 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result, err := p.core.DKeys(arg0)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseStringSlicePayload(stringsSliceToBytesSlise(result))
	case "HGETALL":
		if request.ArgumentsLen() != 1 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result, err := p.core.DGetAll(arg0)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseStringSlicePayload(result)
	case "HDEL":

		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}
		arg1, err := request.GetArgumentVariadicString(1)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result, err := p.core.DDel(arg0, arg1)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseIntPayload(result)
	case "LLEN":
		if request.ArgumentsLen() != 1 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result, err := p.core.LLen(arg0)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseIntPayload(result)
	case "LRANGE":
		if request.ArgumentsLen() != 3 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

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
		if request.ArgumentsLen() != 2 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

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
		if request.ArgumentsLen() != 3 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

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

		result, err := p.core.LPush(arg0, arg1)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseIntPayload(result)
	case "LPOP":
		if request.ArgumentsLen() != 1 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

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
		if request.ArgumentsLen() != 1 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

		arg0, err := request.GetArgumentString(0)
		if err != nil {
			return getResponseInvalidArguments(request.Cmd, err)
		}

		result, err := p.core.Ttl(arg0)
		if err != nil {
			return getResponseCommandError(request.Cmd, err)
		}

		return getResponseIntPayload(result)
	case "EXPIRE":
		if request.ArgumentsLen() != 2 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

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
		if request.ArgumentsLen() != 1 {
			return getResponseInvalidArguments(request.Cmd, fmt.Errorf("wrong number of arguments for '%s' command: %d", request.Cmd, request.ArgumentsLen()))
		}

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
	case "SETEX":
		seconds, err := request.GetArgumentInt(1)
		if err != nil {
			return err
		}

		seconds -= int(time.Now().Unix() - request.Timestamp)
		request.Args[1] = []byte(strconv.Itoa(seconds))
	case "EXPIRE":
		seconds, err := request.GetArgumentInt(1)
		if err != nil {
			return err
		}

		seconds -= int(time.Now().Unix() - request.Timestamp)
		request.Args[1] = []byte(strconv.Itoa(seconds))
	default:
		//do nothing. Just a placeholder to save correct syntax w/o ttl-related commands
	}

	return nil
}
