# postgresql shape 数据入库工具

## 帮助命令

pgsql2shp -h

## shp2sql
示例：./pgsql2shp -w shp2sql -shp ./shp/aaasssddd.shp -sql ./shp/xxx.sql

## shp2data
示例：pgsql2shp -w sql2data -u postgres -p postgres -h test.diit.cn -P 6454 -table xxx_t -