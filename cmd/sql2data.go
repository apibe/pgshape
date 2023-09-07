package cmd

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"os"
	"regexp"
	"strings"
	"time"
)

const (
	SERVER_NAME = "{SERVER_NAME}"         // 数据库连接地址
	SERVER_TYPE = "{SERVER_TYPE}"         // 数据库类型
	HOST        = "{HOST}"                // 数据库连接地址
	DATABASE    = "{DATABASE}"            // 数据库名称
	SCHEMA      = "{SCHEMA}"              // 模式名称
	DATE        = "{02/01/2006 15:04:05}" // SQL生成时间
	TABLE       = "{TABLE}"               // 数据表名称
	CREATE_SQL  = "{CREATE_SQL}"          // 建表语句
	INSERT_SQL  = "{INSERT_SQL}"          // 插入表语句
	SELF_ADD    = "{SELF_ADD}"            // 自增id函数名称
	PRIMARY_KEY = "{PRIMARY_KEY}"         // 主键名称
	NOTE_VIEW   = "{NOTE_VIEW}"           // 标注视图
)

var SQLModel = `/*
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
-- View of {TABLE}_note
-- ----------------------------
{NOTE_VIEW}

-- ----------------------------
-- Records of {TABLE}
-- ----------------------------
{INSERT_SQL}
`

const (
	FMT_INSERT_SQL = iota
	FMT_CREATE_TABLE_SQL
	FMT_DROP_TABLE_SQL
	FMT_CREATE_VIEW_SQL
	FMT_DROP_VIEW_SQL
	FMT_PRIMARY_KEY_SQL
	FMT_TABLE_OWN_SQL
)

type Content []string

func Load(sqlPath string) Content {
	file, _ := os.ReadFile(sqlPath)
	content := strings.Split(string(file), "\n")
	for i := 0; i < len(content); i++ {
		content[i] = strings.TrimPrefix(content[i], " ")
	}
	return content
}

func newsSql(sql int, str ...interface{}) string {
	const (
		fmt_insert_sql       = `INSERT INTO "%s"."%s" (%s) VALUES (%s);`
		fmt_create_table_sql = `CREATE TABLE "%s"."%s" (%s);`
		fmt_drop_table_sql   = `DROP TABLE IF EXISTS "%s"."%s" CASCADE;`
		fmt_create_view_sql  = `CREATE VIEW "%s"."%s" AS(SELECT %s FROM "%s"."%s");`
		fmt_drop_view_sql    = `DROP VIEW IF EXISTS "%s"."%s";`
		fmt_primary_key_sql  = `ALTER TABLE "%s"."%s" ADD CONSTRAINT "%s_pkey" PRIMARY KEY ("%s");`
		fmt_table_own_sql    = `ALTER TABLE "%s"."%s" OWNER TO "%s";`
	)
	output := ""
	switch sql {
	case FMT_INSERT_SQL:
		output = fmt.Sprintf(fmt_insert_sql, str...)
	case FMT_DROP_TABLE_SQL:
		output = fmt.Sprintf(fmt_drop_table_sql, str...)
	case FMT_CREATE_TABLE_SQL:
		output = fmt.Sprintf(fmt_create_table_sql, str...)
	case FMT_TABLE_OWN_SQL:
		output = fmt.Sprintf(fmt_table_own_sql, str...)
	case FMT_DROP_VIEW_SQL:
		output = fmt.Sprintf(fmt_drop_view_sql, str...)
	case FMT_CREATE_VIEW_SQL:
		output = fmt.Sprintf(fmt_create_view_sql, str...)
	case FMT_PRIMARY_KEY_SQL:
		output = fmt.Sprintf(fmt_primary_key_sql, str...)
	}
	return output
}

type Opinions struct {
	UserName   string
	Password   string
	SourcePort string
	SourceHost string
	Schema     string
	Database   string
	TableName  string
}

type MapShpOpinions struct {
	Name string
}

func (c *Collection) ParseSQL(output string, opt Opinions) {
	// 读取模板SQL文件
	sql := strings.ReplaceAll(SQLModel, SERVER_NAME, opt.SourceHost+":"+opt.SourcePort)
	sql = strings.ReplaceAll(sql, SERVER_TYPE, "Postgresql")
	sql = strings.ReplaceAll(sql, HOST, opt.SourceHost)
	sql = strings.ReplaceAll(sql, DATABASE, opt.Database)
	sql = strings.ReplaceAll(sql, SCHEMA, opt.Schema)
	sql = strings.ReplaceAll(sql, DATE, time.Now().Format(DATE))
	sql = strings.ReplaceAll(sql, TABLE, opt.TableName)
	createSql := fmt.Sprintf("%s \n%s \n%s ", c.CreateTableSql.DropIfExistSql, c.CreateTableSql.CreateSql, c.CreateTableSql.TableOwnSql)
	sql = strings.ReplaceAll(sql, CREATE_SQL, createSql)
	insert := ""
	for _, is := range c.InsertSql.InsertSql {
		insert = fmt.Sprintf("%s \n %s", insert, is)
	}
	insert = fmt.Sprintf("BEGIN;\n%s\nCOMMIT;", insert)
	sql = strings.ReplaceAll(sql, INSERT_SQL, insert)
	sql = strings.ReplaceAll(sql, SELF_ADD, "selfAdd")
	viewSql := fmt.Sprintf("%s \n%s ", c.CreateViewSql.DropIfExistSql, c.CreateViewSql.CreateSql)
	sql = strings.ReplaceAll(sql, NOTE_VIEW, viewSql)
	newFile, _ := os.Create(output)
	_, _ = newFile.Write([]byte(sql))
}

type Information struct {
	SourceServer   string
	ServerType     string
	SourceHost     string
	SourceDatabase string
	SourceTable    string
	SourceSchema   string
}

func (c Content) Information() Information {
	start, end := 0, 0
	info := Information{}
	rgp, _ := regexp.Compile("^ (.* .*): (.*)$")
	for i, c1 := range c {
		if c1 == "/*" {
			start = i
		}
		if c1 == "*/" {
			end = i
		}
	}
	for i := start; i < end+1; i++ {
		if rgp.MatchString(c[i]) {
			args := rgp.FindStringSubmatch(c[i])
			if len(args) > 2 {
				k := strings.ReplaceAll(args[1], " ", "")
				v := strings.ReplaceAll(args[2], " ", "")

				switch k {
				case "SourceServer":
					info.SourceServer = v
				case "SourceServerType":
					info.ServerType = v
				case "SourceHost":
					info.SourceHost = v
				case "SourceDatabase":
					info.SourceDatabase = v
				case "SourceTable":
					info.SourceTable = v
				case "SourceSchema":
					info.SourceSchema = v
				}
			}
		}
	}
	return info
}

type CreateTableSql struct {
	DropIfExistSql string
	CreateSql      string
	TableOwnSql    string
}

func (c Content) CreateTableSql() CreateTableSql {
	cts := CreateTableSql{}
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

type CreateViewSql struct {
	DropIfExistSql string
	CreateSql      string
}

func (c Content) CreateViewSql() CreateViewSql {
	cts := CreateViewSql{}
	endRxp, _ := regexp.Compile("^(.*;)(.*)$")
	dies, _ := regexp.Compile("^DROP VIEW IF EXISTS .*;")
	cs, _ := regexp.Compile("^CREATE VIEW .*;")
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
	}
	return cts
}

type InsertSql struct {
	InsertSql   []string
	Transaction bool
}

func (c Content) InsertSql() InsertSql {
	ins := InsertSql{}
	isql := make([]string, 0)
	rgx, _ := regexp.Compile("^INSERT .*;")
	for i := 0; i < len(c); i++ {
		if rgx.MatchString(c[i]) {
			isql = append(isql, c[i])
		}
	}
	fmt.Println(len(isql))
	ins.Transaction = true
	ins.InsertSql = isql
	return ins
}

func (c Content) PrimaryKey() string {
	//rgx, _ := regexp.Compile(`ALTER TABLE (.*) ADD CONSTRAINT (.*) PRIMARY KEY (.*);`)
	return ""
}

type Collection struct {
	Information    Information
	CreateTableSql CreateTableSql
	InsertSql      InsertSql
	CreateViewSql  CreateViewSql
	PrimaryKey     string
}

func (c Content) Collection() Collection {
	return Collection{
		c.Information(),
		c.CreateTableSql(),
		c.InsertSql(),
		c.CreateViewSql(),
		c.PrimaryKey(),
	}
}

func (c Collection) ClientAndExec(opt Opinions) error {
	var err error
	//参数根据自己的数据库进行修改
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", opt.SourceHost, opt.SourcePort, opt.UserName, opt.Password, opt.Database, "disable"))
	err = db.Ping()
	if err != nil {
		return err
	}
	// 创建表和视图
	tx, _ := db.Begin()
	_, err = tx.Exec(c.CreateTableSql.DropIfExistSql)
	if err != nil {
		return errors.New(fmt.Sprintf("删除表失败,err:%s", err.Error()))
	}
	_, err = tx.Exec(c.CreateTableSql.CreateSql)
	if err != nil {
		return errors.New(fmt.Sprintf("创建表失败,err:%s", err.Error()))
	}
	_, err = tx.Exec(c.CreateTableSql.TableOwnSql)
	if err != nil {
		return errors.New(fmt.Sprintf("创建表所属人失败,err:%s", err.Error()))
	}
	_, err = tx.Exec(c.CreateViewSql.DropIfExistSql)
	if err != nil {
		return errors.New(fmt.Sprintf("删除视图失败,err:%s", err.Error()))
	}
	_, err = tx.Exec(c.CreateViewSql.CreateSql)
	//fmt.Println(c.CreateViewSql.CreateSql)
	if err != nil {
		return errors.New(fmt.Sprintf("创建视图失败,err:%s", err.Error()))
	}
	// 执行insert语句
	if c.InsertSql.Transaction {
		for i := 0; i < len(c.InsertSql.InsertSql); i++ {
			_, err := tx.Exec(c.InsertSql.InsertSql[i])
			if err != nil {
				tx.Rollback()
				return errors.New(fmt.Sprintf("执行第 %d 条insert语句失败，err：%s", i, err.Error()))
			}
		}
		err = tx.Commit()
		return err
	}
	return nil
}

func (m MapShp) ClientAndExec(opt Opinions) (int, error) {
	client, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", opt.SourceHost, opt.SourcePort, opt.UserName, opt.Password, opt.Database, "disable"))
	if err != nil {
		return 0, err
	}
	defer client.Close()
	err = client.Ping()
	id := 0
	client.QueryRow(`INSERT INTO "public"."map_shp" ("name", "show_name", "headers", "type", "box", "create_time", "table_name", "data_catalog_id", "user_id", "user_name", "geom_name", "dsn", "map_shp_id" )
VALUES
	($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13 ) RETURNING ID;`, m.Name, m.ShowName, m.Headers.String(), m.Type, m.Box, m.CreateTime.Format("2006-01-02 15:04:05"), m.TableName, m.DataCatalogId, m.UserId, m.UserId, m.GeomName, m.Dsn, m.MapShpId).Scan(&id)

	return id, nil
}
