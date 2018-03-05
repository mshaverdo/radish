package core

func (e *StorageHash) SetData(data map[string]*Item) {
	for k, v := range data {
		e.AddOrReplaceOne(k, v)
	}
}

func (e *StorageHash) Data() map[string]*Item {
	result := make(map[string]*Item)
	for b := range e.data {
		for k, v := range e.data[b] {
			result[k] = v
		}
	}

	return result
}
