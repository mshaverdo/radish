package core

// Engine incapsulates concrete storage engine  -- Btree, hashmap, etc
type Engine interface {
}

// Core provides domain operations on the storage -- get, set, keys, hset, hdel, etc
type Core struct {
	engine Engine
}

// NewCore constructs new core instance
func NewCore() *Core {
	return &Core{engine: NewHashEngine()}
}
