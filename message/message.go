package message

// Request is a container, represents a Command, parsed from external API interface
type Request struct {
	// Cmd is a Command type
	Cmd string
	// Args is a list of command positional args
	Args []string
	// Meta is a dict of named parameters such as TTL
	Meta map[string]string
	// Payload carrys value for SET/HSET/etc command
	Payload []byte
	// MultiPayloads intended for MSET/HMSET and other bulk commands
	MultiPayloads map[string][]byte
}

// NewRequest constructs new Request object
func NewRequest(cmd string, args []string, meta map[string]string, payload []byte, multiPayloads map[string][]byte) *Request {
	if len(payload) > 0 && len(multiPayloads) > 0 {
		panic("Logic error: unable to use BOTH payload AND multiPayloads simultaneously")
	}
	return &Request{Cmd: cmd, Args: args, Meta: meta, Payload: payload, MultiPayloads: multiPayloads}
}

type Status int

const (
	StatusOk Status = iota
	StatusNotFound
)

// Response is a container, represents a Response to Request Command
type Response struct {
	Status Status
	// Payload carrys value for SET/HSET/etc command
	Payload []byte
	// MultiPayloads intended for MSET/HMSET and other bulk commands
	MultiPayloads map[string][]byte
}

// NewResponse constructs new Response object
func NewResponse(status Status, payload []byte, multiPayloads map[string][]byte) *Response {
	if len(payload) > 0 && len(multiPayloads) > 0 {
		panic("Logic error: unable to use BOTH payload AND multiPayloads simultaneously")
	}

	return &Response{Status: status, Payload: payload, MultiPayloads: multiPayloads}
}
