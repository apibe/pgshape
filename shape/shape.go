package shape

import (
	"database/sql"
	"errors"
	"fmt"
	goShp "github.com/jonas-p/go-shp"
	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/encoding/wkt"
	"golang.org/x/text/encoding/simplifiedchinese"
	"log"
	"os"
	"strings"
)

const srid = 4326

type shp struct {
	*Collection
}

func (c *shp) read(name string) error {
	if !strings.HasSuffix(name, ".shp") {
		return errors.New("invalid parameter shpPath")
	}
	if _, err := os.Stat(name); err != nil {
		return err
	}
	shape, _ := goShp.Open(name)
	fields := shape.Fields()
	copyCreateTableSql := ""
	createViewStr := ""
	copyCreateTableSqlFlag := true
	insertSQLs := make([]string, 0)
	for shape.Next() {
		n, p := shape.Shape()
		keyStr := ""
		valStr := ""
		polygonStr := c.transPolygonStr(shape.GeometryType, p)
		keyStr, valStr = c.transInsertSql(keyStr, valStr, "geom", polygonStr, 0)
		for k, f := range fields {
			fieldName := string(utf8([]byte(strings.Replace(string(f.Name[:]), "\x00", "", -1))))
			attribute := string(utf8([]byte(strings.ReplaceAll(shape.ReadAttribute(n, k), "\u0000", ""))))
			keyStr, valStr = c.transInsertSql(keyStr, valStr, fieldName, attribute, f.Fieldtype)
			if copyCreateTableSqlFlag {
				key := strings.ToLower(fieldName)
				createViewStr += fieldName + ",\n"
				copyCreateTableSql = c.transCreateSql(copyCreateTableSql, f.Fieldtype, f.Size, f.Precision, key)
			}
		}
		insertSQLs = append(insertSQLs, fmt.Sprintf(`INSERT INTO "%s"."%s" (%s) VALUES(%s);`, c.Information.SourceSchema, c.Information.SourceTable, strings.TrimPrefix(keyStr, ","), strings.TrimPrefix(valStr, ",")))
		copyCreateTableSqlFlag = false
	}
	c.createTableSql.CreateSql = newsSql(fmtCreateTableSql, c.Information.SourceSchema, c.Information.SourceTable, "\nfid SERIAL primary key, \n"+copyCreateTableSql+"geom geometry\n")
	c.createTableSql.DropIfExistSql = newsSql(fmtDropTableSql, c.Information.SourceSchema, c.Information.SourceTable)
	c.createTableSql.TableOwnSql = newsSql(fmtTableOwnSql, "public", c.Information.SourceTable, c.Information.UserName)
	c.insertSql.InsertSql = insertSQLs
	return nil
}

func (c *shp) write(name string) error {
	// 1. 连接数据库
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		c.Information.SourceHost, c.Information.SourcePort, c.Information.UserName, c.Information.Password, c.Information.SourceDatabase, "disable"))
	defer db.Close()
	err = db.Ping()
	if err != nil {
		log.Println(err.Error())
	}

}

func (*shp) transPolygonStr(geometryType goShp.ShapeType, shape goShp.Shape) string {
	//fmt.Println(shape.BBox())
	polygonStr := ""
	switch geometryType {
	case goShp.POINT:
		geometry := (shape).(*goShp.Point)
		linestring := geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{geometry.X, geometry.Y}).SetSRID(srid)
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

			multiLineString := geom.NewMultiLineString(geom.XY).MustSetCoords(coord2s).SetSRID(srid)
			polygonStr, _ = wkt.Marshal(multiLineString)
		} else { //linestring
			coords := []geom.Coord{}
			for _, point := range geometry.Points[0:] {
				coords = append(coords, []float64{point.X, point.Y})
			}

			linestring := geom.NewLineString(geom.XY).MustSetCoords(coords).SetSRID(srid)
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

			polygon := geom.NewPolygon(geom.XY).MustSetCoords(coords).SetSRID(srid)
			polygonStr, _ = wkt.Marshal(polygon)
		}
	}
	return polygonStr
}

func (*shp) transCreateSql(createSql string, fieldType byte, size uint8, precision uint8, fieldName string) string {
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
		fidSql := "fid bigserial primary key not null,\n"
		createSql = fidSql + createSql
	}
	return createSql
}

func (*shp) transInsertSql(keyStr, valStr, key, val string, valType byte) (string, string) {
	if key == "geom" {
		keyStr = fmt.Sprintf("%s,%s", keyStr, key)
		valStr = fmt.Sprintf("%s,ST_GeomFromText('%s' , %d)", valStr, val, srid)
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
