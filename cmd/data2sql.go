package cmd

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
)

func ClientAndStruct(querySql string, opt Opinions) (*Collection, error) {
	d := data2sql{}
	col := Collection{}
	// 1. 连接数据库
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", opt.SourceHost, opt.SourcePort, opt.UserName, opt.Password, opt.Database, "disable"))
	err = db.Ping()
	if err != nil {
		return nil, err
	}
	// 2. 根据querySql创建临时表
	tableName := fmt.Sprintf("%s_%s", opt.TableName, time.Now().Format("20060102150405"))
	createLinShiSql := fmt.Sprintf(`CREATE TABLE public.%s AS %s`, tableName, querySql)
	//tx, _ := db.Begin()
	_, err = db.Exec(createLinShiSql)
	if err != nil {
		fmt.Println(err.Error())
	}
	// 3. 查询表信息
	createTableSql, createViewSql, columnTypes := d.transCreateSql(db, tableName, opt.Schema)
	col.CreateTableSql.CreateSql = createTableSql
	col.CreateTableSql.TableOwnSql = newsSql(FMT_TABLE_OWN_SQL, "public", tableName, "postgres")
	col.CreateTableSql.DropIfExistSql = newsSql(FMT_DROP_TABLE_SQL, "public", tableName)
	col.CreateViewSql.CreateSql = createViewSql
	col.CreateViewSql.DropIfExistSql = newsSql(FMT_DROP_VIEW_SQL, "public", tableName+"_note")
	if err != nil {
		fmt.Println(err.Error())
	}
	// 3. 查询所有字段，构造insertsql
	valSql := fmt.Sprintf(`SELECT * FROM %s`, tableName)
	rows, _ := db.Query(valSql)
	defer rows.Close()
	columns, _ := rows.Columns()
	insertSqls := make([]string, 0)
	for rows.Next() {
		values := make([]interface{}, len(columns))
		pointers := make([]interface{}, len(columns))
		for i := range pointers {
			pointers[i] = &values[i]
		}
		err = rows.Scan(pointers...)
		insertSql := d.transInsertSql(columnTypes, tableName, "public", values)
		insertSqls = append(insertSqls, insertSql)
	}
	col.InsertSql.InsertSql = insertSqls
	col.InsertSql.Transaction = true
	// 5. 查询主键信息。构造主键
	_, err = db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", tableName))
	return &col, err
}

type columnTypes struct {
	Name    string `json:"name,omitempty"`
	SqlType string `json:"sql_type,omitempty"`
	Type    string `json:"type,omitempty"`
	Length  int    `json:"length,omitempty"`
	Comment string `json:"comment,omitempty"`
}

type data2sql struct{}

func ClientAndMapShp(showName, userId, userName string, opt Opinions) (*MapShp, error) {
	m := MapShp{}
	m.Dsn = fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable", opt.UserName, opt.Password, opt.SourceHost, opt.SourcePort, opt.Database)
	m.TableName = opt.TableName
	m.Schema = opt.Schema
	m.ShowName = showName
	m.CreateTime = time.Now()
	m.UserId = userId
	m.UserName = userName
	m.DataCatalogId = 0
	m.MapShpId = 0
	columnSql := fmt.Sprintf(`SELECT A.attname AS NAME, format_type ( A.atttypid, A.atttypmod ) AS sql_type, T.typname AS TYPE, A.atttypmod AS LENGTH, col_description ( A.attrelid, A.attnum ) AS COMMENT FROM pg_class C, pg_attribute A, pg_type T, pg_namespace N WHERE C.relname = '%s' AND A.attnum > 0 AND A.attrelid = C.oid AND A.atttypid = T.oid AND N.nspname = '%s'`, opt.TableName, opt.Schema)
	client, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s", opt.SourceHost, opt.SourcePort, opt.UserName, opt.Password, opt.Database, "disable"))
	defer client.Close()
	err = client.Ping()
	if err != nil {
		return nil, err
	}
	rows, _ := client.Query(columnSql)
	coltypes := make([]columnTypes, 0)
	headers := make([]header, 0)
	rgx, _ := regexp.Compile("numeric|int|float")
	defer rows.Close()
	for rows.Next() {
		columnType := columnTypes{}
		_ = rows.Scan(&columnType.Name, &columnType.SqlType, &columnType.Type, &columnType.Length, &columnType.Comment)
		coltypes = append(coltypes, columnType)
		if columnType.Type == "geometry" {
			m.GeomName = columnType.Name
			if err := m.qualityInspection(client, columnType.Name, opt.TableName); err != nil {
				return nil, err
			}
		} else {
			header := header{}
			header.Name = columnType.Name
			if rgx.MatchString(columnType.Type) {
				header.Class = "float"
			} else {
				header.Class = "text"
			}
			if columnType.Comment != "" {
				header.ShowName = columnType.Comment
			} else {
				header.ShowName = columnType.Name
			}
			headers = append(headers, header)
		}
	}
	m.Headers, _ = json.Marshal(headers)
	return &m, nil
}

func (m *MapShp) qualityInspection(client *sql.DB, geomName string, tableName string) error {
	//查询GeometryType
	sqlGeom := fmt.Sprintf("SELECT distinct ST_GeometryType(%s) AS geometry FROM %s where %s is not null", geomName, tableName, geomName)
	rows, err := client.Query(sqlGeom)
	if err != nil {
		return errors.New("矢量数据为空")
	}
	geometryTypes := make([]string, 0)
	for rows.Next() {
		geometryType := ""
		rows.Scan(&geometryType)
		geometryTypes = append(geometryTypes, geometryType)
	}
	//一张表中出现多个GeometryType
	if len(geometryTypes) == 0 {
		return errors.New("矢量数据为空")
	}
	//一张表中出现多个GeometryType
	//if len(geometryTypes) > 1 {
	//	return errors.New(fmt.Sprintf("矢量数据存在多个GeometryType：%s", geometryTypes))
	//}
	//判断投影信息
	srid := make([]string, 0)
	sqlSrid := fmt.Sprintf("select distinct ST_SRID(%s) FROM  %s where %s is not null", geomName, tableName, geomName)
	client.QueryRow(sqlSrid).Scan(&srid)
	if err != nil {
		return errors.New("矢量数据没有投影信息")
	}
	//表中存在多个投影消息
	if len(srid) > 1 {
		return errors.New(fmt.Sprintf("矢量数据存在多个投影：%s", srid))
	}
	//没有投影信息
	if len(srid) == 1 && srid[0] == "0" {
		return errors.New("矢量数据没有投影信息")
	}
	var geometryType string = ""
	if len(geometryTypes) == 1 {
		geometryType = geometryTypes[0]
		switch geometryType {
		case "ST_Polygon", "ST_MultiPolygon":
			geometryType = "polygon"
		case "ST_Point", "ST_MultiPoint":
			geometryType = "point"
		case "ST_MultiLineString", "ST_LinearRing", "ST_LineString":
			geometryType = "polyline"
		default:
			geometryType = ""
		}
		m.Type = geometryType
	}
	// 查询box
	boxSql := fmt.Sprintf("select ST_Extent(%s)as box from %s", geomName, tableName)
	box := ""
	client.QueryRow(boxSql).Scan(&box)
	box = strings.ReplaceAll(strings.ReplaceAll(strings.ReplaceAll(box, "BOX(", ""), ")", ""), " ", ",")
	m.Box = box
	return nil
}

func (data2sql) transCreateSql(client *sql.DB, tableName string, schema string) (string, string, []columnTypes) {
	columnSql := fmt.Sprintf(`SELECT A.attname AS NAME, format_type ( A.atttypid, A.atttypmod ) AS sql_type, T.typname AS TYPE, A.atttypmod AS LENGTH, col_description ( A.attrelid, A.attnum ) AS COMMENT FROM pg_class C, pg_attribute A, pg_type T, pg_namespace N WHERE C.relname = '%s' AND A.attnum > 0 AND A.attrelid = C.oid AND A.atttypid = T.oid AND N.nspname = '%s'`, tableName, schema)
	rows, _ := client.Query(columnSql)
	coltypes := make([]columnTypes, 0)
	createArgStr := ""
	createViewStr := ""
	defer rows.Close()
	for rows.Next() {
		columnType := columnTypes{}
		_ = rows.Scan(&columnType.Name, &columnType.SqlType, &columnType.Type, &columnType.Length, &columnType.Comment)
		coltypes = append(coltypes, columnType)
		createArgStr += fmt.Sprintf("%s %s,\n", columnType.Name, columnType.SqlType)
		if columnType.Type == "geometry" {
			createViewStr += fmt.Sprintf("st_area ( ST_GeometryN ( %s, generate_series ( 1, ST_NumGeometries ( %s ) ) ) ) AS st_area,\n", columnType.Name, columnType.Name)
			createViewStr += fmt.Sprintf("ST_PointOnSurface ( ST_GeometryN ( %s, generate_series ( 1, ST_NumGeometries ( %s ) ) ) ) AS geom,\n", columnType.Name, columnType.Name)
		} else {
			createViewStr += fmt.Sprintf("%s,\n", columnType.Name)
		}
	}
	createArgStr = strings.TrimSuffix(createArgStr, ",\n")
	createViewStr = strings.TrimSuffix(createViewStr, ",\n")
	createTableSql := newsSql(FMT_CREATE_TABLE_SQL, schema, tableName, createArgStr)
	createViewSql := newsSql(FMT_CREATE_VIEW_SQL, schema, tableName+"_note", createViewStr, schema, tableName)
	return createTableSql, createViewSql, coltypes
}

func (data2sql) transInsertSql(columnTypes []columnTypes, tableName string, schema string, values []interface{}) string {
	//numberRexp, _ := regexp.Compile("int|float|numeric")
	argStr := ""
	valStr := ""
	for i := 0; i < len(columnTypes); i++ {
		argStr += fmt.Sprintf("%s,", columnTypes[i].Name)
		if columnTypes[i].Type == "geometry" {
			valStr += fmt.Sprintf("'%s',", values[i])
		} else {
			rgx, _ := regexp.Compile("int|numeric|float")
			if rgx.MatchString(columnTypes[i].Type) {
				valStr += fmt.Sprintf("%v,", getV(values[i]))
			} else {
				valStr += fmt.Sprintf("'%v',", getV(values[i]))
			}
		}
	}
	argStr = strings.TrimSuffix(argStr, ",")
	valStr = strings.TrimSuffix(valStr, ",")
	return fmt.Sprintf(`INSERT INTO "%s"."%s" (%s) VALUES (%s);`, schema, tableName, argStr, valStr)
}

func getV(v interface{}) interface{} {
	switch v.(type) {
	case string:
	case []byte:
		return string(v.([]byte))
	default:
		return v
	}
	return v
}
