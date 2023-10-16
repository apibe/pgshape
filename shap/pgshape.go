package shape

import (
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"strings"
)

// pgshape 支持数据类型
// geojson gpkg xlsx shp

// 空间数据类型互转

// ReadFile 文件数据读取，支持 geojson gpkg xlsx shp
func ReadFile(name string, info *Information, opt *Opinions) (*Collection, error) {
	c := &Collection{
		Information:    info,
		createTableSql: &createTableSql{},
		insertSql:      &insertSql{},
		Opinions:       opt,
	}
	if strings.HasSuffix(name, ".shp") {
		shp := &shp{c}
		err := shp.read(name)
		return c, err
	} // 读取 shp 文件
	//if strings.HasSuffix(name, ".sql") {
	//	postgresql := &pgsql{c}
	//	err := postgresql.read(name)
	//	return c, err
	//} // 读取 sql 文件
	if strings.HasSuffix(name, ".geojson") {
	} // 读取 geojson 文件
	if strings.HasSuffix(name, ".xlsx") {
	} // 读取 xlsx 文件
	if strings.HasSuffix(name, ".gpkg") {
	} // 读取 gpkg 文件
	return nil, errors.New("无法解析该数据类型！")
}

// Read pg数据读取
func Read(query string, info *Information) (*Collection, error) {
	c := &Collection{
		Information:    info,
		createTableSql: &createTableSql{},
		insertSql:      &insertSql{},
		Opinions:       DefaultOpinions(),
	}
	err := c.query(query)
	return c, err
}

func (c *Collection) WriteFile(name string) error {
	if strings.HasSuffix(name, ".shp") {
		shp := shp{c}
		return shp.write(name)
	} // 写入 shp 文件
	//if strings.HasSuffix(name, ".sql") {
	//	pgsql := pgsql{c}
	//	return pgsql.write(name)
	//} // 写入 sql 文件
	if strings.HasSuffix(name, ".geojson") {

	} // 写入 geojson 文件
	if strings.HasSuffix(name, ".xlsx") {

	} // 写入 xlsx 文件
	if strings.HasSuffix(name, ".gpkg") {

	} // 写入 gpkg 文件
	return errors.New(fmt.Sprintf("未知的数据类型 %s ！", name))
}

// Write 数据写入pg数据库,支持geojson gpkg xlsx shp
func (c *Collection) Write() error {
	return c.exec()
}
