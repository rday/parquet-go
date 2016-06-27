package parquet

import (
	"fmt"
	"io"
	"strings"

	"github.com/TuneLab/parquet-go/parquet/thrift"
)

// ColumnChunkReader provides methods to read values stored from a single
// parquet column chunk.
type ColumnChunkReader interface {
	Next() bool
	Levels() Levels
	Value() interface{}
	Err() error
}

type countingReader struct {
	rs io.ReadSeeker
	n  int64
}

func (r *countingReader) Read(p []byte) (n int, err error) {
	n, err = r.rs.Read(p)
	r.n += int64(n)
	return
}

// NewColumnChunkReader creates a ColumnChunkReader to read cc from r.
func NewColumnChunkReader(r io.ReadSeeker, cs ColumnDescriptor, cc thrift.ColumnChunk) (ColumnChunkReader, error) {
	if ccName := strings.Join(cc.MetaData.PathInSchema, "."); ccName != cs.SchemaElement.Name {
		return nil, fmt.Errorf("column schema for %s and column chunk for %s do not match", cs.SchemaElement.Name, ccName)
	}
	switch cs.SchemaElement.GetType() {
	case thrift.Type_BOOLEAN:
		return newBooleanColumnChunkReader(r, cs, cc)
	case thrift.Type_BYTE_ARRAY:
		return newByteArrayColumnChunkReader(r, cs, cc)
	default:
		return nil, fmt.Errorf("Type %s not yet supported", cs.SchemaElement.GetType())
	}
}
