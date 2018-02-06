package core

type HashEngine struct {
	data map[string]Item
}

func NewHashEngine() *HashEngine {
	return &HashEngine{data: make(map[string]Item)}
}
