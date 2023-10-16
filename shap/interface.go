package shape

type Reader interface {
	Read() (*Collection, error) // 读取文件
	Write()
}

type Writer interface {
}

//
//func Read(name string) Reader {
//	if strings.HasSuffix(name, ".sql") {
//		reader := &pgReader{}
//		reader.setContent(name)
//	}
//}
