package parquet

import (
	"flag"
	"fmt"
	"runtime/pprof"
	"os"
)

/*  Profiling to build a call graph. Work aroung the testing faill. */

// Profiling stuff ... from http://blog.golang.org/profiling-go-programs
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")


func main(){
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			fmt.Println("Error: ", err)
		}
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}


	for i := 0; i < 50000; i++ {
		testBooleanColumnChunkReader()
	}
}

type cell struct {
	d int
	r int
	v interface{}
}

func checkColumnValues(path string, columnIdx int, expected []cell) {

	fd, err := OpenFile(path)
	if err != nil {
		fmt.Errorf("failed to read %s: %s", path, err)
		return
	}
	defer fd.Close()
	// schema, err := schemaFromFileMetaData(m)
	// if err != nil {
	// 	t.Errorf("failed to create schema: %s", err)
	// 	return
	// }

	schema := fd.Schema()
	columns := schema.Columns()
	scanner, err := fd.ColumnScanner(columns[columnIdx])

	if err != nil {
		fmt.Println("Fatal error: ",err)
	}

	// for i, rg := range m.RowGroups {
	// 	cc := rg.Columns[c]
	// 	columnSchema := schema.ColumnByPath(cc.MetaData.PathInSchema)
	// 	//var cr ColumnChunkReader
	//switch cs.SchemaElement.GetType() {
	//case parquetformat.Type_BOOLEAN:
	//cr, err = NewBooleanColumnChunkReader(r, cs, cc)
	//case parquetformat.Type_BYTE_ARRAY:
	//cr, err = NewByteArrayColumnChunkReader(r, cs, cc)
	//}

	// scanner := NewColumnScanner(r, cc, columnSchema.SchemaElement)

	for scanner.Scan() {

		//buffer := make([]bool, 0, 4)

		// if k < len(expected) {
		// 	// got := cell{cr.Levels().D, cr.Levels().R, cr.Value()}
		// 	// if !reflect.DeepEqual(got, expected[k]) {
		// 	// 	t.Errorf("column %d: value at pos %d = %#v, want %#v", c, k, got, expected[k])
		// 	// }
		// }

		// scanner.BoolArray()

		// k += len(buffer)
		//fmt.Printf("V:%v\tD:%d\tR:%d\n", cr.Value(), cr.Levels().D, cr.Levels().R)
	}

	if scanner.Err() != nil {
		fmt.Errorf("column %d: failed to read row group: %s", columnIdx, scanner.Err())
	}

	// if k != len(expected) {
	// 	t.Errorf("column %d: read %d values, want %d values", c, k, len(expected))
	// }
}

func testBooleanColumnChunkReader() {
	checkColumnValues( "testdata/Booleans.parquet", 0, []cell{
		{0, 0, true},
		{0, 0, true},
		{0, 0, false},
		{0, 0, true},
		{0, 0, false},
		{0, 0, true},
	})

	checkColumnValues( "testdata/Booleans.parquet", 1, []cell{
		{0, 0, false},
		{1, 0, false},
		{1, 0, true},
		{1, 0, true},
		{0, 0, false},
		{1, 0, true},
	})

	checkColumnValues( "testdata/Booleans.parquet", 2, []cell{
		{0, 0, false},

		{0, 0, false},

		{1, 0, true},

		{1, 0, true},
		{1, 1, false},
		{1, 1, true},

		{0, 0, false},
		{1, 0, true},
	})
}
