package producer

import (
	"bytes"
	"sync"
)

// used for better performance by reducing the GC workload
var buffers = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func acquireBuffer() *bytes.Buffer {
	return buffers.Get().(*bytes.Buffer)
}

func releaseBuffer(buff *bytes.Buffer) {
	buff.Reset()
	buffers.Put(buff)
}
