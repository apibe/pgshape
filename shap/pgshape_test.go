package shape

import (
	"fmt"
	"testing"
)

var col *Collection

func init() {
	col, _ = ReadFile("/Users/apple/Desktop/diit/workspace/apibe/pgshape/shp/aaasssddd.shp",
		&Information{
			UserName:       "postgres",
			Password:       "bjsh",
			SourcePort:     "5432",
			SourceHost:     "apibe.top",
			SourceSchema:   "public",
			SourceDatabase: "online_make_map",
			SourceTable:    "xxx_table",
		},
		DefaultOpinions(),
	)
}

func TestRead(t *testing.T) {
	read, _ := Read("SELECT * FROM xxx_table", &Information{
		UserName:       "postgres",
		Password:       "bjsh",
		SourcePort:     "5432",
		SourceHost:     "apibe.top",
		SourceSchema:   "public",
		SourceDatabase: "online_make_map",
		SourceTable:    "xxx_table",
	})
	fmt.Println(read.Content.Values[0]["fid"])
}
