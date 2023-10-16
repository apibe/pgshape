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

func TestReadFile(t *testing.T) {

	WriteFile("/Users/apple/Desktop/diit/workspace/apibe/pgshape/shp/aaasssddd.sql", col)
}

func TestWrite(t *testing.T) {
	err := Write(col)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestQuery(t *testing.T) {
	_ = col.query("SELECT * FROM xxx_table")
}
