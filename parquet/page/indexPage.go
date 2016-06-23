package page

import "github.com/TuneLab/Parquet-go/parquet/thrift"

//IndexPage
type IndexPage struct {
	header *thrift.IndexPageHeader
}

func NewIndexPage(header *thrift.IndexPageHeader) *IndexPage {
	return &IndexPage{header: header}
}
