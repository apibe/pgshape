package main

import (
	"encoding/json"
	"fmt"
	"server/tools/pgsql2shp/cmd"
	"testing"
)

func TestMergeShp(t *testing.T) {
	opt := cmd.Opinions{
		UserName:   "postgres",
		Password:   "postgres",
		SourcePort: "6454",
		SourceHost: "test.diit.cn",
		Schema:     "public",
		Database:   "online_make_map_test",
		TableName:  "table_uuid",
	}
	shpAndStruct, err := cmd.ShpAndStruct("/Users/apple/go/src/server/tools/pgsql2shp/shp/aaasssddd.shp", opt)
	if err != nil {
		panic(err.Error())
	}
	shpAndStruct.ParseSQL("/Users/apple/go/src/server/tools/pgsql2shp/sql/new2.sql", opt)
}

func TestMergeData(t *testing.T) {
	opt := cmd.Opinions{
		UserName:   "postgres",
		Password:   "postgres",
		SourcePort: "6454",
		SourceHost: "test.diit.cn",
		Schema:     "public",
		Database:   "online_make_map_test",
		TableName:  "table_uuid",
	}
	clientAndStruct, err := cmd.ClientAndStruct("SELECT * FROM table_uuid", opt)
	if err != nil {
		panic(err.Error())
	}
	clientAndStruct.ParseSQL("/Users/apple/go/src/server/tools/pgsql2shp/sql/new3.sql", opt)
}

func TestMergeSqlToData(t *testing.T) {
	opt := cmd.Opinions{
		UserName:   "postgres",
		Password:   "postgres",
		SourcePort: "6454",
		SourceHost: "test.diit.cn",
		Schema:     "public",
		Database:   "online_make_map_test",
		TableName:  "table_uuid",
	}
	err := cmd.Load("/Users/apple/go/src/server/tools/pgsql2shp/sql/new3.sql").Collection().ClientAndExec(opt)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func TestShpAndMapShp(t *testing.T) {
	opt := cmd.Opinions{
		UserName:   "postgres",
		Password:   "postgres",
		SourcePort: "6454",
		SourceHost: "test.diit.cn",
		Schema:     "public",
		Database:   "online_make_map_test",
		TableName:  "table_uuid",
	}
	shp := cmd.ShpAndMapShp("/Users/apple/go/src/server/tools/pgsql2shp/shp/aaasssddd.shp", "postgres://postgres:postgres@211.103.138.154:6454/pc-pg_v_1_2?sslmode=disable", opt)
	fmt.Println(string(shp.Headers))
}

func TestClientAndMapShp(t *testing.T) {
	opt := cmd.Opinions{
		UserName:   "postgres",
		Password:   "postgres",
		SourcePort: "6454",
		SourceHost: "test.diit.cn",
		Schema:     "public",
		Database:   "online_make_map_test",
		TableName:  "table_uuid",
	}
	shp, err := cmd.ClientAndMapShp("哦y", "123", "测试", opt)
	if err != nil {
		panic(err.Error())
	}
	bytes, err := json.Marshal(shp)
	id, err := shp.ClientAndExec(opt)
	if err != nil {
		panic(err.Error())
	}
	fmt.Println(id)
	fmt.Println(string(bytes))
}
