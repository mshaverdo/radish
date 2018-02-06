package core

//go:generate stringer -type=ItemKind
type ItemKind int

const (
	String ItemKind = iota
	List
	Dict
)

type Item struct {
	kind ItemKind
	str  string
	list []string
	dict map[string]string
}

func (i *Item) Kind() ItemKind {
	return i.kind
}

func (i *Item) Str() string {
	if i.kind != String {
		panic("Program Logic error: trying to get Str value on " + i.kind.String())
	}
	return i.str
}

func (i *Item) List() []string {
	if i.kind != List {
		panic("Program Logic error: trying to get List value on " + i.kind.String())
	}
	return i.list
}

func (i *Item) Dict() map[string]string {
	if i.kind != Dict {
		panic("Program Logic error: trying to get Dict value on " + i.kind.String())
	}
	return i.dict
}
