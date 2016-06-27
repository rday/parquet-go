package parquet

import "testing"
// import _ "net/http/pprof"

/*  This file is currently for exploration purposes.
    We repeat a test enough times to get a cpu profile, then
    utilize pprof tool to build a call graph.
*/


func TestProfileBooleanColumnChunkReader(t *testing.T) {
	for i := 0; i < 50000; i++ {
		TestBooleanColumnChunkReader(t)
	}
}
