package consumer

// store kafka messages to cache before sending to channel
type Cache struct {
	storage []*any
	offset  int
}

func NewMsgCache(capacity int) *Cache {
	return &Cache{
		storage: make([]*any, 0, capacity),
		offset:  0,
	}
}

func (c *Cache) Append(msg *any) {
	c.storage = append(c.storage, msg)
}

func (c *Cache) HasNext() bool {
	return c.Len() > c.offset
}

func (c *Cache) Len() int {
	return len(c.storage)
}

func (c *Cache) Next() *any {
	defer c.IncrementOffset()
	return c.storage[c.offset]
}

func (c *Cache) ResetOffset() {
	c.offset = 0
}

func (c *Cache) IncrementOffset() {
	c.offset++
}

func (c *Cache) ResetCache() {
	c.storage = c.storage[:0]
}
