package core

func (e *StorageHash) SetData(data map[string]*Item) {
	e.data = data
}

func (e *StorageHash) Data() map[string]*Item {
	return e.data
}
