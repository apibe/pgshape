package main

import (
	"flag"
	"fmt"
	"server/tools/pgsql2shp/cmd"
	"strings"
	"time"
)

const (
	sql2data = "sql2data"
	data2sql = "data2sql"
	shp2sql  = "shp2sql"
	shp2data = "shp2data"
)

var (
	work     = flag.String("w", "shp2sql", "功能拆分：sql2data|data2sql|shp2sql|shp2data")
	shpPath  = flag.String("shp", "./new.shp", "shp文件路径")
	sqlPath  = flag.String("sql", "./new.sql", "sql文件路径")
	query    = flag.String("q", "", "数据库查询语句")
	host     = flag.String("h", "", "数据库host地址")
	port     = flag.String("p", "", "数据库连接端口port")
	user     = flag.String("u", "", "数据库账号")
	password = flag.String("P", "", "数据库密码")
	schema   = flag.String("schema", "", "数据库所属模式")
	database = flag.String("db", "", "数据库名")
	table    = flag.String("table", "", "数据表名")
)

type FLAG struct {
	Work     string
	ShpPath  string
	SqlPath  string
	Query    string
	Host     string
	Port     string
	User     string
	Password string
	Database string
	Schema   string
	Table    string
}

func main() {
	flag.Parse()
	f := FLAG{}
	f.Work = *work
	f.ShpPath = *shpPath
	f.SqlPath = *sqlPath
	f.Query = *query
	f.Host = *host
	f.Port = *port
	f.User = *user
	f.Password = *password
	f.Database = *database
	f.Schema = *schema
	f.Table = *table
	opt := f.init()
	switch f.Work {
	case shp2sql:
		sas, err := cmd.ShpAndStruct(f.ShpPath, opt)
		if err != nil {
			panic(err)
		}
		sas.ParseSQL(f.SqlPath, opt)
	case shp2data:
		sas, err := cmd.ShpAndStruct(f.ShpPath, opt)
		if err != nil {
			panic(err)
		}
		err = sas.ClientAndExec(opt)
	case data2sql:
		cas, err := cmd.ClientAndStruct(f.Query, opt)
		if err != nil {
			panic(err)
		}
		cas.ParseSQL(f.SqlPath, opt)
	case sql2data:
		err := cmd.Load(f.SqlPath).Collection().ClientAndExec(opt)
		if err != nil {
			panic(err)
		}
	default:
		panic("shp2sql|data2sql|sql2data")
	}

}

func (f *FLAG) init() cmd.Opinions {
	if f.Work == "" {
		panic("sql2data|data2sql|shp2sql")
	}
	opt := cmd.Opinions{}
	switch f.Work {
	case shp2sql:
		if f.ShpPath == "" {
			panic("shpPath is null")
		} else {
			if f.Table == "" {
				ls := strings.Split(f.ShpPath, "/")
				f.Table = strings.TrimSuffix(ls[len(ls)-1], ".shp")
			}
		}
		if f.SqlPath == "" {
			f.SqlPath = "./new.sql"
		}
		if f.Schema == "" {
			f.Schema = "public"
		}
		if f.Database == "" {
			f.Database = "online_make_map"
		}
		opt.TableName = f.Table
		opt.Schema = f.Schema
		opt.SourceHost = f.Host
		opt.Database = f.Database
	case data2sql:
		if f.SqlPath == "" {
			f.SqlPath = "./new.sql"
		}
		if f.Port == "" || f.Host == "" || f.User == "" || f.Password == "" {
			panic("参数无效")
		}
		if f.Table == "" && f.Schema == "" && f.Query == "" {
			panic("查询参数无效")
		}
		if f.Query != "" {
			if f.Table == "" {
				f.Table = "table_" + time.Now().Format("20060102150405")
			}
			if f.Schema == "" {
				f.Schema = "public"
			}
		}
		if f.Query == "" {
			if f.Table != "" && f.Schema != "" {
				f.Query = fmt.Sprintf(`SELECT * FROM "%s"."%s"`, f.Schema, f.Table)
			} else {
				panic("参数无效")
			}
		}
		opt.TableName = f.Table
		opt.Schema = f.Schema
		opt.SourceHost = f.Host
		opt.Database = f.Database
	case sql2data:
		if f.Port == "" || f.Host == "" || f.User == "" || f.Password == "" || f.SqlPath == "" {
			panic("参数无效")
		}
		if f.Database == "" {
			panic("请指定数据库名称")
		}
		if f.Table == "" {
			panic("请指定表格导入表名称")
		}
		opt.TableName = f.Table
		opt.Schema = f.Schema
		opt.SourceHost = f.Host
		opt.Database = f.Database
	case shp2data:
		if f.Port == "" || f.Host == "" || f.User == "" || f.Password == "" || f.ShpPath == "" {
			panic("参数无效")
		}
		if f.ShpPath == "" {
			panic("请指定待导入shp文件地址")
		}
		if f.Database == "" {
			panic("请指定数据库名称")
		}
		if f.Table == "" {
			panic("请指定表格导入表名称")
		}
		opt.TableName = f.Table
		opt.Schema = f.Schema
		opt.SourceHost = f.Host
		opt.Database = f.Database
	}
	return opt
}
