package main

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/TuneLab/parquet-go/parquet"
	"github.com/linkedin/goavro"
)

func check(w int, err error) {
	if err != nil {
		log.Fatal(err)
	}
}

const schema = `
{
    "type": "record",
    "name": "example",
    "fields": [
        {
            "type": "long",
            "name": "long_field"
        },
        {
            "type": "int",
            "name": "integer_field"
        },
        {
            "type": "string",
            "name": "decimal_field"
        },
        {
            "type": "float",
            "name": "float_field"
        },
        {
            "type": "double",
            "name": "double_field"
        },
        {
            "type": "boolean",
            "name": "boolean_field"
        },
        {
            "type": "string",
            "name": "string_field"
        },
        {
            "type": "long",
            "name": "date_field"
        },
        {
            "type": "long",
            "name": "timestamp_field"
        }
    ]
}
    `

// use Avro
func makeSomeData(w io.Writer) error {
	var err error
	codec, err := goavro.NewCodec(schema)
	if err != nil {
		log.Fatal(err)
	}

	fw, err := codec.NewWriter(
		//		goavro.BlockSize(13),
		goavro.Compression(goavro.CompressionSnappy),
		goavro.ToWriter(w))

	if err != nil {
		log.Fatal("avro: cannot create Writer: ", err)
	}
	defer fw.Close()

	// GENERATE A Record based on the type.
	rec := map[string]interface{}{"long_field": int64(1), "integer_field": int32(2), "decimal_field": string(3),
		"float_field": float32(4), "double_field": float64(5), "boolean_field": true,
		"string_field": string("7"), "date_field": int64(8), "timestamp_field": int64(9),
	}

	for i := 0; i < 100; i++ {

		record, err := goavro.NewRecord(goavro.RecordSchema(schema))
		if err != nil {
			log.Fatal(err)
		}

		for k, v := range rec {
			record.Set(k, v)
		}

		fw.Write(record)
	}

	return nil
}

func dumpReader(r io.Reader) {
	fr, err := goavro.NewReader(goavro.BufferFromReader(r))
	if err != nil {
		log.Fatal("cannot create Reader: ", err)
	}
	defer func() {
		if err := fr.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	for fr.Scan() {
		datum, err := fr.Read()
		if err != nil {
			log.Println("cannot read datum: ", err)
			continue
		}
		fmt.Println(datum)
	}
}

func main() {
	fd, err := os.Create("temp.avro")
	if err != nil {
		log.Println("error", err)
	}

	makeSomeData(fd)

	fd.Close()

	fd, err = os.Open("temp.avro")
	if err != nil {
		log.Println("error", err)
	}

	//dumpReader(fd)

	fd.Close()

	record, err := goavro.NewRecord(goavro.RecordSchema(schema))
	if err != nil {
		log.Println("error", err)
	}
	{
		fd, err = os.Create("temp.parquet")
		if err != nil {
			log.Println("error", err)
		}

		pschema := parquet.NewSchema()

		for _, f := range record.Fields {

			fschema, err := record.GetFieldSchema(f.Name)
			if err != nil {
				panic(err)
			}

			err = pschema.AddColumnFromThriftSchema(fschema.(map[string]interface{}))
			if err != nil {
				panic(err)
			}
		}

		enc := parquet.NewEncoder(pschema, fd)

		// Read the file
		fd, err = os.Open("temp.avro")
		if err != nil {
			log.Println("error", err)
		}

		fr, err := goavro.NewReader(goavro.BufferFromReader(fd))
		if err != nil {
			log.Fatal("cannot create Reader: ", err)
		}

		defer func() {
			if err := fr.Close(); err != nil {
				log.Fatal(err)
			}
		}()

		for fr.Scan() {
			datum, err := fr.Read()
			if err != nil {
				log.Println("cannot read datum: ", err)
				continue
			}
			r, ok := datum.(*goavro.Record)
			if !ok {
				panic("expected goavro.Record")
			}

			rec := make(map[string]interface{})
			for _, field := range r.Fields {
				v, err := r.Get(field.Name)
				if err != nil {
					panic(err)
				}
				rec[field.Name] = v
			}

			err = enc.WriteRecords([]map[string]interface{}{rec})
			if err != nil {
				panic(err)
			}
		}

		if err := enc.Close(); err != nil {
			panic(err)
		}

		fd.Close()
	}

	log.Println("finished")
}
