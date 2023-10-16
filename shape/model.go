package shape

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
)

const (
	model_name        = "{SERVER_NAME}"         // 数据库连接地址
	model_type        = "{SERVER_TYPE}"         // 数据库类型
	model_host        = "{HOST}"                // 数据库连接地址
	model_database    = "{DATABASE}"            // 数据库名称
	model_schema      = "{SCHEMA}"              // 模式名称
	model_date        = "{02/01/2006 15:04:05}" // SQL生成时间
	model_table       = "{TABLE}"               // 数据表名称
	model_create_sql  = "{CREATE_SQL}"          // 建表语句
	model_insert_sql  = "{INSERT_SQL}"          // 插入表语句
	model_self_add    = "{SELF_ADD}"            // 自增id函数名称
	model_primary_key = "{PRIMARY_KEY}"         // 主键名称
)
const (
	fmtInsertSql = iota
	fmtCreateTableSql
	fmtDropTableSql
	fmtPrimaryKeySql
	fmtTableOwnSql
)

var sqlModel = `/*
 ONLINE-MAP-MAKING Premium Data Transfer

 Source Server         : {SERVER_NAME}
 Source Server Type    : {SERVER_TYPE}
 Source Host           : {HOST}
 Source Catalog        : {DATABASE}
 Source Table          : {TABLE}
 Source Schema         : {SCHEMA}

 Date: {02/01/2006 15:04:05}
*/


-- ----------------------------
-- Table structure for {TABLE}
-- ----------------------------
{CREATE_SQL}

-- ----------------------------
-- Records of {TABLE}
-- ----------------------------
{INSERT_SQL}
`

type (
	Collection struct {
		*Information
		*Opinions
		*Content

		createTableSql *createTableSql
		insertSql      *insertSql
		rows           *sql.Rows
	}
	Information struct {
		UserName       string
		Password       string
		SourcePort     string
		SourceHost     string
		SourceSchema   string
		SourceDatabase string
		SourceTable    string
	}
	createTableSql struct {
		DropIfExistSql string
		CreateSql      string
		TableOwnSql    string
	}
	insertSql struct {
		InsertSql []string
	}
	Context struct {
		BeforeCreateSql []string
		AfterCreateSql  []string
		BeforeInsertSql []string
		AfterInsertSql  []string
	}
	Opinions struct {
		Cover   bool // 建表是否覆盖，默认关闭
		Specify struct {
			Float  string
			String string
		} // 数值类型指定|字符串类型指定 默认都是 text
		Context // 上下文执行的SQL
	}
	Content struct {
		Name               []string
		ColumnType         []reflect.Type
		DatabaseColumnType []string
		Values             [][]interface{}
	}
)

func DefaultOpinions() *Opinions {
	return &Opinions{
		Cover: false,
		Specify: struct {
			Float  string
			String string
		}{
			"text",
			"text",
		},
		Context: Context{},
	}
}

func newsSql(sql int, str ...interface{}) string {
	const (
		fmt_insert_sql       = `INSERT INTO "%s"."%s" (%s) VALUES (%s);`
		fmt_create_table_sql = `CREATE TABLE "%s"."%s" (%s);`
		fmt_drop_table_sql   = `DROP TABLE IF EXISTS "%s"."%s" CASCADE;`
		fmt_primary_key_sql  = `ALTER TABLE "%s"."%s" ADD CONSTRAINT "%s_pkey" PRIMARY KEY ("%s");`
		fmt_table_own_sql    = `ALTER TABLE "%s"."%s" OWNER TO "%s";`
	)
	output := ""
	switch sql {
	case fmtInsertSql:
		output = fmt.Sprintf(fmt_insert_sql, str...)
	case fmtDropTableSql:
		output = fmt.Sprintf(fmt_drop_table_sql, str...)
	case fmtCreateTableSql:
		output = fmt.Sprintf(fmt_create_table_sql, str...)
	case fmtTableOwnSql:
		output = fmt.Sprintf(fmt_table_own_sql, str...)
	case fmtPrimaryKeySql:
		output = fmt.Sprintf(fmt_primary_key_sql, str...)
	}
	return output
}

func (c *Collection) exec() error {
	// 1. 连接数据库
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		c.Information.SourceHost, c.Information.SourcePort, c.Information.UserName, c.Information.Password, c.Information.SourceDatabase, "disable"))
	defer db.Close()
	err = db.Ping()
	if err != nil {
		return err
	}
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	run := func(tx *sql.Tx, query ...string) error {
		for _, q := range query {
			if q != "" {
				_, err = tx.Exec(q)
				if err != nil {
					tx.Rollback()
					return err
				}
			}
		}
		return nil
	}
	// 执行sql
	err = run(tx, c.BeforeCreateSql...)
	if err != nil {
		log.Println(1)
		return err
	}
	if c.Cover {
		err = run(tx, c.createTableSql.DropIfExistSql)
	}
	err = run(tx, c.createTableSql.CreateSql)
	if err != nil {
		return err
	}
	err = run(tx, c.createTableSql.TableOwnSql)
	if err != nil {
		return err
	}
	err = run(tx, c.AfterCreateSql...)
	if err != nil {
		return err
	}
	err = run(tx, c.BeforeInsertSql...)
	if err != nil {
		return err
	}
	err = run(tx, c.insertSql.InsertSql...)
	if err != nil {
		return err
	}
	err = run(tx, c.AfterInsertSql...)
	if err != nil {
		return err
	}
	tx.Commit()
	return err
}

func (c *Collection) query(query string) error {
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		c.Information.SourceHost, c.Information.SourcePort, c.Information.UserName, c.Information.Password, c.Information.SourceDatabase, "disable"))
	defer db.Close()
	err = db.Ping()
	if err != nil {
		return err
	}
	c.rows, err = db.Query(query)
	return err
}
