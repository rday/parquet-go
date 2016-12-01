package encoding

import (
	"bufio"
	"fmt"
	"io"

	"github.com/TuneLab/parquet-go/parquet/datatypes"
	"github.com/TuneLab/parquet-go/parquet/encoding/rle"
)

type plainDictionaryDecoder struct {
	rb         *bufio.Reader
	dictionary Dictionary
	count      uint
}

type Dictionary interface {
	MapBool(keys []uint32, out []bool) error
	MapInt32(keys []uint32, out []int32) error
	MapInt64(keys []uint32, out []int64) error
	MapInt96(keys []uint32, out []datatypes.Int96) error
	MapByteArray(keys []uint32, out [][]byte) error
	MapFloat32(keys []uint32, out []float32) error
	MapFloat64(keys []uint32, out []float64) error
}

func NewPlainDictionaryDecoder(r io.Reader, dictionary Dictionary, numValues uint) Decoder {
	if dictionary == nil {
		panic("null dictionary")
	}
	return &plainDictionaryDecoder{rb: bufio.NewReader(r), dictionary: dictionary, count: numValues}
}

func (d *plainDictionaryDecoder) readKeys() ([]uint32, error) {
	bitWidth, err := d.rb.ReadByte()
	if err != nil {
		return nil, err
	}

	keys, err := rle.ReadUint32(d.rb, uint(bitWidth), d.count)

	if err != nil {
		return nil, fmt.Errorf("rle: could not read %d values with bitWidth %d: %s", d.count, uint(bitWidth), err)
	}

	return keys, nil
}

func (d *plainDictionaryDecoder) DecodeBool(out []bool) (uint, error) {
	keys, err := d.readKeys()
	if err != nil {
		return 0, fmt.Errorf("could not read dictionary keys: %s", err)
	}
	return uint(len(keys)), d.dictionary.MapBool(keys, out)
}

func (d *plainDictionaryDecoder) DecodeInt32(out []int32) (uint, error) {
	keys, err := d.readKeys()
	if err != nil {
		return 0, fmt.Errorf("could not read dictionary keys: %s", err)
	}
	return uint(len(keys)), d.dictionary.MapInt32(keys, out)
}

func (d *plainDictionaryDecoder) DecodeInt64(out []int64) (uint, error) {
	keys, err := d.readKeys()
	if err != nil {
		return 0, fmt.Errorf("could not read dictionary keys: %s", err)
	}
	return uint(len(keys)), d.dictionary.MapInt64(keys, out)
}

func (d *plainDictionaryDecoder) DecodeInt96(out []datatypes.Int96) (uint, error) {
	keys, err := d.readKeys()
	if err != nil {
		return 0, fmt.Errorf("could not read dictionary keys: %s", err)
	}
	return uint(len(keys)), d.dictionary.MapInt96(keys, out)
}

func (d *plainDictionaryDecoder) DecodeFloat32(out []float32) (uint, error) {
	keys, err := d.readKeys()
	if err != nil {
		return 0, fmt.Errorf("could not read dictionary keys: %s", err)
	}
	return uint(len(keys)), d.dictionary.MapFloat32(keys, out)
}

func (d *plainDictionaryDecoder) DecodeFloat64(out []float64) (uint, error) {
	keys, err := d.readKeys()
	if err != nil {
		return 0, fmt.Errorf("could not read dictionary keys: %s", err)
	}
	return uint(len(keys)), d.dictionary.MapFloat64(keys, out)
}

func (d *plainDictionaryDecoder) DecodeByteArray(out [][]byte) (uint, error) {
	keys, err := d.readKeys()
	if err != nil {
		return 0, fmt.Errorf("could not read dictionary keys: %s", err)
	}

	return uint(len(keys)), d.dictionary.MapByteArray(keys, out)
}

func (d *plainDictionaryDecoder) DecodeFixedByteArray(out [][]byte, _ uint) (uint, error) {
	keys, err := d.readKeys()
	if err != nil {
		return 0, fmt.Errorf("could not read dictionary keys: %s", err)
	}

	return uint(len(keys)), d.dictionary.MapByteArray(keys, out)
}

func (d *plainDictionaryDecoder) String() string {
	return "plainDictionaryDecoder"
}
