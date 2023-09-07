package cmd

import (
	"encoding/json"
	"errors"
	"fmt"
	goShp "github.com/jonas-p/go-shp"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkt"
	"golang.org/x/text/encoding/simplifiedchinese"
	"os"
	"strings"
	"time"
)

func ShpAndStruct(shpPath string, opt Opinions) (*Collection, error) {
	col := Collection{}
	s := shp2sql{}
	if !strings.HasSuffix(shpPath, ".shp") {
		return nil, errors.New("invalid parameter shpPath")
	}
	if _, err := os.Stat(shpPath); err != nil {
		return nil, err
	}
	shape, _ := goShp.Open(shpPath)
	fields := shape.Fields()
	copyCreateTableSql := ""
	createViewStr := ""
	copyCreateTableSqlFlag := true
	insertSQLs := make([]string, 0)
	for shape.Next() {
		n, p := shape.Shape()
		keyStr := ""
		valStr := ""
		polygonStr := s.transPolygonStr(shape.GeometryType, p)
		keyStr, valStr = s.transInsertSql(keyStr, valStr, "geom", polygonStr, 0)
		for k, f := range fields {
			fieldName := string(utf8([]byte(strings.Replace(string(f.Name[:]), "\x00", "", -1))))
			attribute := string(utf8([]byte(strings.ReplaceAll(shape.ReadAttribute(n, k), "\u0000", ""))))
			keyStr, valStr = s.transInsertSql(keyStr, valStr, fieldName, attribute, f.Fieldtype)
			if copyCreateTableSqlFlag {
				key := strings.ToLower(fieldName)
				createViewStr += fieldName + ",\n"
				copyCreateTableSql = s.transCreateSql(copyCreateTableSql, f.Fieldtype, f.Size, f.Precision, key)
			}
		}
		insertSQLs = append(insertSQLs, fmt.Sprintf(`INSERT INTO "%s"."%s" (%s) VALUES(%s);`, opt.Schema, opt.TableName, strings.TrimPrefix(keyStr, ","), strings.TrimPrefix(valStr, ",")))
		copyCreateTableSqlFlag = false
	}
	createViewStr += fmt.Sprintf("st_area ( ST_GeometryN ( %s, generate_series ( 1, ST_NumGeometries ( %s ) ) ) ) AS st_area,\n", "geom", "geom")
	createViewStr += fmt.Sprintf("ST_PointOnSurface ( ST_GeometryN ( %s, generate_series ( 1, ST_NumGeometries ( %s ) ) ) ) AS geom\n", "geom", "geom")
	col.CreateTableSql.CreateSql = newsSql(FMT_CREATE_TABLE_SQL, opt.Schema, opt.TableName, "\nfid bigserial primary key not null, \n"+copyCreateTableSql+"geom geometry\n")
	col.CreateTableSql.DropIfExistSql = newsSql(FMT_DROP_TABLE_SQL, opt.Schema, opt.TableName)
	col.CreateTableSql.TableOwnSql = newsSql(FMT_TABLE_OWN_SQL, "public", opt.TableName, "postgres")
	col.CreateViewSql.CreateSql = newsSql(FMT_CREATE_VIEW_SQL, opt.Schema, opt.TableName+"_note", createViewStr, opt.Schema, opt.TableName)
	col.CreateViewSql.DropIfExistSql = newsSql(FMT_DROP_VIEW_SQL, "public", opt.TableName+"_note")
	col.InsertSql.InsertSql = insertSQLs
	col.InsertSql.Transaction = true
	return &col, nil
}

type MapShp struct {
	ID            uint      `gorm:"primary_key" json:"id,omitempty"`
	Name          string    `gorm:"type:varchar(100)" json:"name,omitempty"`
	ShowName      string    `gorm:"type:varchar(100)" json:"showname,omitempty"`
	Headers       JSON      `gorm:"type:json" json:"headers,omitempty"`
	Type          string    `gorm:"type:varchar(20)" json:"type,omitempty"`
	Box           string    `gorm:"type:varchar(255)" json:"box,omitempty"`
	Schema        string    `json:"schema"`
	TableName     string    `json:"tableName"`
	GeomName      string    `json:"geomName"`
	Dsn           string    `json:"dsn"`
	MapShpId      uint      `json:"mapShpId"`
	DataCatalogId uint      `json:"dataCatalogId"`
	UserId        string    `json:"userId"`
	UserName      string    `json:"userName"`
	CreateTime    time.Time `json:"createTime"`
}
type header struct {
	Name     string `json:"name"`
	ShowName string `json:"showname"`
	Class    string `json:"class"`
}

func ShpAndMapShp(shpPath, dsn string, opt Opinions) MapShp {
	mapshp := MapShp{}
	shape, _ := goShp.Open(shpPath)
	fields := shape.Fields()
	headers := make([]header, 0)
	headers = append(headers, header{
		Name:     "fid",
		ShowName: "fid",
		Class:    "primaryKey",
	}) // 默认的主键id
	for _, field := range fields {
		if field.Fieldtype == 78 || field.Fieldtype == 70 {
			headers = append(headers, header{
				Name:     string(utf8([]byte(strings.Replace(string(field.Name[:]), "\x00", "", -1)))),
				ShowName: string(utf8([]byte(strings.Replace(string(field.Name[:]), "\x00", "", -1)))),
				Class:    "float",
			})
		} else {
			headers = append(headers, header{
				Name:     string(utf8([]byte(strings.Replace(string(field.Name[:]), "\x00", "", -1)))),
				ShowName: string(utf8([]byte(strings.Replace(string(field.Name[:]), "\x00", "", -1)))),
				Class:    "text",
			})
		}
	}
	data, _ := json.Marshal(headers)
	mapshp.Headers = data
	box := shape.BBox()
	mapshp.Box = fmt.Sprintf("%f,%f,%f,%f", box.MinX, box.MinY, box.MaxX, box.MaxY)
	mapshp.Name = opt.TableName
	mapshp.ShowName = opt.TableName
	mapshp.Dsn = dsn
	switch shape.GeometryType {
	case goShp.POINT, goShp.POINTZ, goShp.POINTM, goShp.MULTIPOINT, goShp.MULTIPOINTM, goShp.MULTIPOINTZ:
		mapshp.Type = "point"
		mapshp.GeomName = "geom"
	case goShp.NULL:
		mapshp.Type = "table"
	case goShp.POLYGON, goShp.POLYGONM, goShp.POLYGONZ:
		mapshp.Type = "polygon"
		mapshp.GeomName = "geom"
	case goShp.POLYLINE, goShp.POLYLINEM, goShp.POLYLINEZ:
		mapshp.GeomName = "geom"
		mapshp.Type = "polyline"
	default:
		mapshp.Type = "table"
	}
	return mapshp
}

type shp2sql struct{}

func (shp2sql) transPolygonStr(geometryType goShp.ShapeType, shape goShp.Shape) string {
	//fmt.Println(shape.BBox())
	polygonStr := ""
	switch geometryType {
	case goShp.POINT:
		geometry := (shape).(*goShp.Point)
		linestring := geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{geometry.X, geometry.Y}).SetSRID(4326)
		polygonStr, _ = wkt.Marshal(linestring)
	case goShp.POLYLINE:
		geometry := (shape).(*goShp.PolyLine)
		//multilinestring
		if geometry.NumParts > 1 {
			coord2s := [][]geom.Coord{}
			for i := int32(0); i < geometry.NumParts; i++ {
				coords := []geom.Coord{}
				if i == geometry.NumParts-1 {
					for _, point := range geometry.Points[geometry.Parts[i]:] {
						coords = append(coords, []float64{point.X, point.Y})
					}
				} else {
					for _, point := range geometry.Points[geometry.Parts[i]:geometry.Parts[i+1]] {
						coords = append(coords, []float64{point.X, point.Y})
					}
				}
				coord2s = append(coord2s, coords)
			}

			multiLineString := geom.NewMultiLineString(geom.XY).MustSetCoords(coord2s).SetSRID(4326)
			polygonStr, _ = wkt.Marshal(multiLineString)
		} else { //linestring
			coords := []geom.Coord{}
			for _, point := range geometry.Points[0:] {
				coords = append(coords, []float64{point.X, point.Y})
			}

			linestring := geom.NewLineString(geom.XY).MustSetCoords(coords).SetSRID(4326)
			polygonStr, _ = wkt.Marshal(linestring)
		}
	case goShp.POLYGON:
		geometry := (shape).(*goShp.Polygon)
		if geometry.NumParts > 1 { // 多面  multiPolygon
			coord2s := [][][]geom.Coord{}
			for i := int32(0); i < geometry.NumParts; i++ {
				coords := [][]geom.Coord{}
				coord := []geom.Coord{}
				if i == geometry.NumParts-1 {
					for _, point := range geometry.Points[geometry.Parts[i]:] {
						coord = append(coord, []float64{point.X, point.Y})
					}
					coords = append(coords, coord)
				} else {

					for _, point := range geometry.Points[geometry.Parts[i]:geometry.Parts[i+1]] {
						coord = append(coord, []float64{point.X, point.Y})
					}
					coords = append(coords, coord)
				}
				coord2s = append(coord2s, coords)
			}
			multiPolygon := geom.NewMultiPolygon(geom.XY).MustSetCoords(coord2s).SetSRID(4490)
			polygonStr, _ = wkt.Marshal(multiPolygon)
		} else { // 面  polygon
			coords := [][]geom.Coord{}
			coord := []geom.Coord{}
			for _, point := range geometry.Points[0:] {
				coord = append(coord, []float64{point.X, point.Y})
			}
			coords = append(coords, coord)

			polygon := geom.NewPolygon(geom.XY).MustSetCoords(coords).SetSRID(4326)
			polygonStr, _ = wkt.Marshal(polygon)
		}
	}
	return polygonStr
}

func (shp2sql) transCreateSql(createSql string, fieldType byte, size uint8, precision uint8, fieldName string) string {
	if fieldName == "geom" {
		return createSql
	}
	if fieldType == 78 || fieldType == 70 {
		if fieldType == 78 {
			number := fmt.Sprintf(" numeric(%d,%d),\n", size, precision)
			createSql += fieldName + number
		} else {
			number := fmt.Sprintf(" float8,\n")
			createSql += fieldName + number
		}
	} else {
		varchar := fmt.Sprintf(" varchar(%d),\n", size)
		createSql += fieldName + varchar
	}
	if fieldName == "fid" {
		fidSql := "fid bigserial  primary key not null,\n"
		createSql = fidSql + createSql
	}
	return createSql
}

func (shp2sql) transInsertSql(keyStr, valStr, key, val string, valType byte) (string, string) {
	if key == "geom" {
		keyStr = fmt.Sprintf("%s,%s", keyStr, key)
		valStr = fmt.Sprintf("%s,ST_GeomFromText('%s' , %d)", valStr, val, 4326)
	} else {
		keyStr = fmt.Sprintf("%s,%s", keyStr, key)
		switch valType {
		case 78, 70:
			valStr = fmt.Sprintf("%s,%s", valStr, val)
		default:
			valStr = fmt.Sprintf("%s,'%s'", valStr, val)
		}
	}
	return keyStr, valStr
}

func (shp2sql) generateSQL(sName, host, source, schema, table, createSql, selfAdd, primaryKey, view string, insertSql []string) {
	// 读取模板SQL文件
	file, _ := os.ReadFile("/Users/apple/go/src/server/tools/pgsql2shp/sql/example.sql")
	sql := strings.ReplaceAll(string(file), SERVER_NAME, sName)
	sql = strings.ReplaceAll(sql, SERVER_TYPE, "Postgresql")
	sql = strings.ReplaceAll(sql, HOST, host)
	sql = strings.ReplaceAll(sql, DATABASE, source)
	sql = strings.ReplaceAll(sql, SCHEMA, schema)
	sql = strings.ReplaceAll(sql, DATE, time.Now().Format(DATE))
	sql = strings.ReplaceAll(sql, TABLE, table)
	createSql = fmt.Sprintf("DROP TABLE IF EXISTS \"%s\".\"%s\" CASCADE;\n%s\nALTER TABLE \"%s\".\"%s\" OWNER TO \"postgres\";",
		schema, table,
		createSql,
		schema, table,
	)
	sql = strings.ReplaceAll(sql, CREATE_SQL, createSql)
	insert := ""
	for _, is := range insertSql {
		insert = fmt.Sprintf("%s \n %s;", insert, is)
	}
	insert = fmt.Sprintf("BEGIN;\n%s\nCOMMIT;", insert)
	sql = strings.ReplaceAll(sql, INSERT_SQL, insert)
	sql = strings.ReplaceAll(sql, SELF_ADD, selfAdd)
	primaryKey = fmt.Sprintf("ALTER TABLE \"%s\".\"%s\" ADD CONSTRAINT \"%s_pkey\" PRIMARY KEY (\"%s\");\n", schema, table, table, primaryKey)
	sql = strings.ReplaceAll(sql, PRIMARY_KEY, primaryKey)
	sql = strings.ReplaceAll(sql, NOTE_VIEW, view)
	newFile, _ := os.Create("/Users/apple/go/src/server/tools/pgsql2shp/sql/new.sql")
	_, _ = newFile.Write([]byte(sql))
}

func preNUm(data byte) int {
	var mask byte = 0x80
	var num int = 0
	//8bit中首个0bit前有多少个1bits
	for i := 0; i < 8; i++ {
		if (data & mask) == mask {
			num++
			mask = mask >> 1
		} else {
			break
		}
	}
	return num
}
func isUtf8(data []byte) bool {
	i := 0
	for i < len(data) {
		if (data[i] & 0x80) == 0x00 {
			// 0XXX_XXXX
			i++
			continue
		} else if num := preNUm(data[i]); num > 2 {
			// 110X_XXXX 10XX_XXXX
			// 1110_XXXX 10XX_XXXX 10XX_XXXX
			// 1111_0XXX 10XX_XXXX 10XX_XXXX 10XX_XXXX
			// 1111_10XX 10XX_XXXX 10XX_XXXX 10XX_XXXX 10XX_XXXX
			// 1111_110X 10XX_XXXX 10XX_XXXX 10XX_XXXX 10XX_XXXX 10XX_XXXX
			// preNUm() 返回首个字节的8个bits中首个0bit前面1bit的个数，该数量也是该字符所使用的字节数
			i++
			for j := 0; j < num-1; j++ {
				//判断后面的 num - 1 个字节是不是都是10开头
				if (data[i] & 0xc0) != 0x80 {
					return false
				}
				i++
			}
		} else {
			//其他情况说明不是utf-8
			return false
		}
	}
	return true
}
func utf8(data []byte) []byte {
	if !isUtf8(data) {
		data, _ = simplifiedchinese.GBK.NewDecoder().Bytes(data)
		return data
	}
	return data
}
