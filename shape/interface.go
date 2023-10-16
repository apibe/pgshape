package shape

import "strings"

type Reader interface {
	ReadFile(name string, info *Information, opt *Opinions) (*Collection, error) // 读取文件
	Read(query string, info *Information) (*Collection, error)                   // 从数据库读取
	CreateSql() *createTableSql                                                  // 生成createSql
	InsertSql() *insertSql                                                       // 生成insertSql
}

type Writer interface {
	WriteFile(name string, col *Collection) error // 写入到文件
	Write(col *Collection) error                  // 写入到数据库
	CreateSql() *createTableSql                   // 生成createSql
	InsertSql() *insertSql                        // 生成insertSql
}

func Read(name string) Reader {
	if strings.HasSuffix(name, ".sql") {
		return &pgReader{
			content:    nil,
			Rows:       nil,
			Collection: nil,
		}
	}
}
