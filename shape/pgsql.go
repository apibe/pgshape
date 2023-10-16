package shape

import (
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"
)

type (
	pgReader struct {
		*content
		*sql.Rows
		*Collection
	}
	content []string

	pgWriter struct {
		*Collection
	}
)

func (pg *pgReader) ReadFile(name string, info *Information, opt *Opinions) (*Collection, error) {
	col := &Collection{
		Information: info,
		Opinions:    opt,
	}
	file, err := os.ReadFile(name)
	con := strings.Split(string(file), "\n")
	for i := 0; i < len(con); i++ {
		con[i] = strings.TrimPrefix(con[i], " ")
	}
	c := content(con)
	pg.content = &c
	col.createTableSql = pg.CreateSql()
	col.insertSql = pg.InsertSql()
	return col, err
}
func (pg *pgReader) Read(query string, info *Information) (*Collection, error) {
	col := &Collection{
		Information: info,
		Opinions:    DefaultOpinions(),
	}
	err := col.query(query)
	pg.Rows = col.rows
	col.createTableSql = pg.CreateSql()
	col.insertSql = pg.InsertSql()
	return col, err
}
func (pg *pgReader) CreateSql() *createTableSql {
	cts := &createTableSql{}
	endRxp, _ := regexp.Compile("^(.*;)(.*)$")
	dies, _ := regexp.Compile("^DROP TABLE IF EXISTS .*;")
	cs, _ := regexp.Compile("^CREATE TABLE .*$")
	tos, _ := regexp.Compile("^ALTER TABLE .* OWNER TO .*;")
	c := *pg.content
	for i := 0; i < len(c); i++ {
		if dies.MatchString(c[i]) {
			cts.DropIfExistSql = c[i]
		}
		if cs.MatchString(c[i]) {
			for j := i; j < len(c); j++ {
				if endRxp.MatchString(c[j]) {
					end := endRxp.FindStringSubmatch(c[j])
					cts.CreateSql = strings.Join(c[i:j], " ") + end[1]
					break
				}
			}
		}
		if tos.MatchString(c[i]) {
			cts.TableOwnSql = c[i]
		}
	}
	return cts
} // 生成createSql
func (pg *pgReader) InsertSql() *insertSql {
	ins := &insertSql{}
	isql := make([]string, 0)
	rgx, _ := regexp.Compile("^INSERT .*;")
	c := *pg.content
	for i := 0; i < len(c); i++ {
		if rgx.MatchString(c[i]) {
			isql = append(isql, c[i])
		}
	}
	ins.InsertSql = isql
	return ins
}

func (c content) createTableSql() *createTableSql {
	cts := &createTableSql{}
	endRxp, _ := regexp.Compile("^(.*;)(.*)$")
	dies, _ := regexp.Compile("^DROP TABLE IF EXISTS .*;")
	cs, _ := regexp.Compile("^CREATE TABLE .*$")
	tos, _ := regexp.Compile("^ALTER TABLE .* OWNER TO .*;")
	for i := 0; i < len(c); i++ {
		if dies.MatchString(c[i]) {
			cts.DropIfExistSql = c[i]
		}
		if cs.MatchString(c[i]) {
			for j := i; j < len(c); j++ {
				if endRxp.MatchString(c[j]) {
					end := endRxp.FindStringSubmatch(c[j])
					cts.CreateSql = strings.Join(c[i:j], " ") + end[1]
					break
				}
			}
		}
		if tos.MatchString(c[i]) {
			cts.TableOwnSql = c[i]
		}
	}
	return cts
}

func (c Content) createTableSql() *createTableSql {}

func (pg *pgWriter) WriteFile(name string, col *Collection) error {
	// 读取模板SQL文件
	sql := strings.ReplaceAll(sqlModel, model_name, col.Information.SourceHost+":"+col.Information.SourcePort)
	sql = strings.ReplaceAll(sql, model_type, "Postgresql")
	sql = strings.ReplaceAll(sql, model_host, col.Information.SourceHost)
	sql = strings.ReplaceAll(sql, model_database, col.Information.SourceDatabase)
	sql = strings.ReplaceAll(sql, model_schema, col.Information.SourceSchema)
	sql = strings.ReplaceAll(sql, model_date, time.Now().Format(model_date))
	sql = strings.ReplaceAll(sql, model_table, col.Information.SourceTable)
	if col.Cover {
		createSql := fmt.Sprintf("%s \n%s \n%s ", col.createTableSql.DropIfExistSql, col.createTableSql.CreateSql, col.createTableSql.TableOwnSql)
		sql = strings.ReplaceAll(sql, model_create_sql, createSql)
	} else {
		createSql := fmt.Sprintf("%s \n%s ", col.createTableSql.CreateSql, col.createTableSql.TableOwnSql)
		sql = strings.ReplaceAll(sql, model_create_sql, createSql)
	}
	insert := ""
	for _, is := range col.insertSql.InsertSql {
		insert = fmt.Sprintf("%s \n %s", insert, is)
	}
	insert = fmt.Sprintf("BEGIN;\n%s\nCOMMIT;", insert)
	sql = strings.ReplaceAll(sql, model_insert_sql, insert)
	newFile, err := os.Create(name)
	_, err = newFile.Write([]byte(sql))
	return err
} // 写入到文件
func (pg *pgWriter) Write(col *Collection) error {
	return col.exec()
}
func (pg *pgWriter) CreateSql() *createTableSql {
	return
} // 生成createSql
func (pg *pgWriter) InsertSql() *insertSql {
	ins := &insertSql{}
	isql := make([]string, 0)
	rgx, _ := regexp.Compile("^INSERT .*;")
	c := *pg.content
	for i := 0; i < len(c); i++ {
		if rgx.MatchString(c[i]) {
			isql = append(isql, c[i])
		}
	}
	ins.InsertSql = isql
	return ins
}
