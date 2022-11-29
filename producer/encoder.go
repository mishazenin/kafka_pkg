package producer

import "io"

type EncoderFn func(msg interface{}, wr io.Writer) error

// TODO in case of need of better performance use sync.Pool
type kafkaByteEncoder []byte

func (k kafkaByteEncoder) Encode() ([]byte, error) {
	return k, nil
}
func (k kafkaByteEncoder) Length() int {
	return len(k)
}

// TODO zero alloc reset method + take a poiter of the struct
func (k kafkaByteEncoder) Release() {
	k = kafkaByteEncoder{}
}
