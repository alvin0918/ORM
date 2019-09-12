package ORM

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"html"
	"strings"
)

type MysqlDBConfig struct {
	where     string
	orderBy   string
	groupBy   string
	join      string
	field     string
	tableName string
	alias     string
	isSql     bool
	query     string
}

var DBConfig *MysqlDBConfig

/**
初始化数据库
*/
func init() {
	DBConfig = &MysqlDBConfig{
		isSql:false,
	}
}

/**
连接数据库
*/
func conn() (db *sql.DB, err error) {

	// 连接MySQL
	if db, err = sql.Open("mysql", "root:abc123456@tcp(127.0.0.1:3306)/ddz?charset=utf8"); err != nil {
		panic(errors.New("数据库连接失败！原因是：" + err.Error()))
	}

	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(100)
	err = db.Ping()

	return
}

/**
设置WHERE条件，支持链式重复调用
*/
func (DBConfig *MysqlDBConfig) Where(str string, mode string) *MysqlDBConfig {
	if DBConfig.where == "" {
		DBConfig.where = " WHERE " + str
	} else {
		DBConfig.where = " " + mode + " " + str
	}

	return DBConfig
}

/**
设置查询字段，支持链式，支持重复调用
*/
func (DBConfig *MysqlDBConfig) Field(str string) *MysqlDBConfig {
	if DBConfig.field == "" {
		DBConfig.field = str
	} else {
		DBConfig.field = ", " + str
	}

	return DBConfig
}

/**
设置排序条件，支持链式，不支持重复调用
*/
func (DBConfig *MysqlDBConfig) OrderBy(str string, mode string) *MysqlDBConfig {

	DBConfig.orderBy = str + " " + mode

	return DBConfig
}

/**
设置数据表
*/
func (DBConfig *MysqlDBConfig) TableName(str string) *MysqlDBConfig {

	DBConfig.tableName = str

	return DBConfig
}

/**
设置数据表
*/
func (DBConfig *MysqlDBConfig) Alias(str string) *MysqlDBConfig {

	DBConfig.alias = str

	return DBConfig
}

/**
设置分组排序，支持链式调用，不支持重复使用
*/
func (DBConfig *MysqlDBConfig) GroupBy(str string, mode string) *MysqlDBConfig {

	DBConfig.orderBy = str + " " + mode

	return DBConfig
}

/**
连表操作，支持链式，支持重复调用
*/
func (DBConfig *MysqlDBConfig) Join(str string, mode string) *MysqlDBConfig {

	switch strings.ToLower(mode) {
	case "inner":
		DBConfig.join = " INNER JOIN " + str
	case "right":
		DBConfig.join = " RIGHT JOIN " + str
	case "left":
		DBConfig.join = " LEFT JOIN " + str
	default:
		panic(errors.New("Abnormal parameter！"))
	}

	return DBConfig
}

/**
是否打印SQL
 */
func (DBConfig *MysqlDBConfig) IsPrintSql(mode bool) *MysqlDBConfig {

	DBConfig.isSql = mode

	return DBConfig
}

/**
查询数据， 返回结果map
*/
func (DBConfig *MysqlDBConfig) Select() (result map[int]map[string]string, err error) {

	var (
		query string
		rows  *sql.Rows
		str   string
		cols  []string
		val   [][]byte
		scans []interface{}
		i     int
		row   map[string]string
		db    *sql.DB
	)

	defer func() {
		err = db.Close()
	}()

	// 每次调用首相初始化一个连接
	if db, err = conn(); err != nil {
		panic(errors.New(err.Error()))
	}

	str = "SELECT"

	// 获取SQL语句
	query = DBConfig.analysisSql(str)

	if rows, err = db.Query(query); err != nil {
		panic(errors.New("查询失败!" + err.Error()))
	}

	// 查出字段
	if cols, err = rows.Columns(); err != nil {
		panic(errors.New("查询失败!" + err.Error()))
	}

	// 查出每一列的值
	val = make([][]byte, len(cols))

	// rows.Scan()的参数， 因为每次查询出来的列是不定长的，用len(cols)定住每次查询的长度
	scans = make([]interface{}, len(cols))

	// 让每一行数据填充到val中
	for i := range val {
		scans[i] = &val[i]
	}

	// 得到最后的map
	result = make(map[int]map[string]string)

	i = 0

	// 循环游标，向下推移
	for rows.Next() {
		if err = rows.Scan(scans...); err != nil {
			panic(errors.New(err.Error()))
		}

		// 获取每一行的数据
		row = make(map[string]string)

		for k, v := range val {
			key := cols[k]
			row[key] = string(v)
		}

		// 装入结果集中
		result[i] = row

		i++
	}

	return
}

/**
指查询一条
 */
func (DBConfig *MysqlDBConfig) Find() (result map[string]string, err error) {

	var (
		rows map[int]map[string]string
	)

	rows, err = DBConfig.Select()

	if len(rows[0]) > 0 {
		result = rows[0]
	}

	return
}

/**
插入数据 isRows true 返回影响的行数 FALSE 返回最后一行的主键ID
*/
func (DBConfig *MysqlDBConfig) Insert(data map[string]string, isRows bool) (rows int64, err error) {

	var (
		query  string
		stmt   *sql.Stmt
		result sql.Result
		str    string
		db     *sql.DB
	)

	defer func() {
		err = db.Close()
	}()

	// 每次调用首相初始化一个连接
	if db, err = conn(); err != nil {
		panic(errors.New(err.Error()))
	}

	str = "INSERT"

	// 获取SQL语句
	query = DBConfig.analysisSqls(data, str)

	if stmt, err = db.Prepare(query); err != nil {
		panic(errors.New(err.Error()))
	}

	if result, err = stmt.Exec(); err != nil {
		panic(errors.New(err.Error()))
	}

	if isRows {
		if rows, err = result.RowsAffected(); err != nil {
			panic(errors.New(err.Error()))
		}
	} else {
		if rows, err = result.LastInsertId(); err != nil {
			panic(errors.New(err.Error()))
		}
	}

	return

}

/**
修改数据 isRows true 返回影响的行数 FALSE 返回最后一行的主键ID
*/
func (DBConfig *MysqlDBConfig) Update(data map[string]string, isRows bool) (rows int64, err error) {

	var (
		query  string
		stmt   *sql.Stmt
		result sql.Result
		str    string
		db     *sql.DB
	)

	defer func() {
		err = db.Close()
	}()

	str = "Upload"

	// 获取SQL语句
	query = DBConfig.analysisSqls(data, str)

	// 每次调用首相初始化一个连接
	if db, err = conn(); err != nil {
		panic(errors.New(err.Error()))
	}

	if stmt, err = db.Prepare(query); err != nil {
		panic(errors.New(err.Error()))
	}

	if result, err = stmt.Exec(); err != nil {
		panic(errors.New(err.Error()))
	}

	if isRows {
		if rows, err = result.RowsAffected(); err != nil {
			panic(errors.New(err.Error()))
		}
	} else {
		if rows, err = result.LastInsertId(); err != nil {
			panic(errors.New(err.Error()))
		}
	}

	return

}

/**
删除数据 isRows true 返回影响的行数 FALSE 返回最后一行的主键ID
*/
func (DBConfig *MysqlDBConfig) Delete(isRows bool) (rows int64, err error) {

	var (
		query  string
		stmt   *sql.Stmt
		result sql.Result
		str    string
		db     *sql.DB
	)

	defer func() {
		err = db.Close()
	}()

	// 每次调用首相初始化一个连接
	if db, err = conn(); err != nil {
		panic(errors.New(err.Error()))
	}

	str = "DELETE"

	// 获取SQL语句
	query = DBConfig.analysisSql(str)

	if stmt, err = db.Prepare(query); err != nil {
		panic(errors.New(err.Error()))
	}

	if result, err = stmt.Exec(); err != nil {
		panic(errors.New(err.Error()))
	}

	if isRows {
		if rows, err = result.RowsAffected(); err != nil {
			panic(errors.New(err.Error()))
		}
	} else {
		if rows, err = result.LastInsertId(); err != nil {
			panic(errors.New(err.Error()))
		}
	}

	return

}

/**
根据查询模式，获取SQL
*/
func (DBConfig *MysqlDBConfig) analysisSql(mode string) (str string) {

	str = strings.ToUpper(mode)

	switch str {
	case "UPDATE":
		if DBConfig.tableName == "" {
			panic(errors.New("不能没有表名呀兄弟！"))
		}

		str += " " + DBConfig.tableName + " SET "

		if DBConfig.field == "" {
			panic(errors.New("需要修改字段及数据"))
		}

		str += " " + DBConfig.field

		if DBConfig.where != "" {
			str += " " + DBConfig.where
		}

	case "DELETE":

		if DBConfig.tableName == "" {
			panic(errors.New("不能没有表名呀兄弟！"))
		}

		str += " FROM " + DBConfig.tableName

		if DBConfig.where != "" {
			str += " " + DBConfig.where
		} else {
			panic(errors.New("这个操作太危险啦！真那么想不开的话设置成 1 = 1吧！"))
		}

	case "SELECT":

		// 格式化查询字段
		if DBConfig.field != "" {
			str += " " + DBConfig.field
		} else {
			str += " * "
		}

		// 设置表名
		if DBConfig.tableName != "" {
			str += " FROM " + DBConfig.tableName
		} else {
			panic(errors.New("Can't Find TableName！"))
		}

		// 设置表别名
		if DBConfig.alias != "" {
			str += " AS " + DBConfig.alias
		}

		// 格式化查询条件
		if DBConfig.where != "" {
			str += " " + DBConfig.where
		}

		// 格式化分组
		if DBConfig.groupBy != "" {
			str += " " + DBConfig.groupBy
		}

		// 格式化排序
		if DBConfig.orderBy != "" {
			str += " " + DBConfig.orderBy
		}
	default:
		// 执行原生SQL
		return DBConfig.query
	}

	// SQL语句格式化，简要避免SQL注入
	str = html.EscapeString(str)

	// 是否打印SQL
	if DBConfig.isSql {
		fmt.Println(str)
	}

	return
}

/**
根据查询模式，获取SQL
*/
func (DBConfig *MysqlDBConfig) analysisSqls(data map[string]string, mode string) (str string) {

	str = strings.ToUpper(mode)

	switch str {
	case "INSERT":

		if DBConfig.tableName == "" {
			panic(errors.New("不能没有表名呀兄弟！"))
		}

		str += " INTO " + DBConfig.tableName

		var key string = "("
		var value string = "("

		for k, v := range data{
			if key == "(" {
				key += k + ","
			}

			if value == "(" {
				value += v
			}
		}

		key += ")"
		value += ")"

		str += " " + key + " VALUES " + value

		if DBConfig.where != "" {
			str += " " + DBConfig.where
		}

	case "UPDATE":
		if DBConfig.tableName == "" {
			panic(errors.New("不能没有表名呀兄弟！"))
		}

		str += " " + DBConfig.tableName + " SET "

		for k, v := range data{
			str += " " + k + " = " + v + ","
		}

		if DBConfig.where != "" {
			str += " " + DBConfig.where
		}

	default:
		// 执行原生SQL
		return DBConfig.query
	}

	// SQL语句格式化，简要避免SQL注入
	str = html.EscapeString(str)

	// 是否打印SQL
	if DBConfig.isSql {
		fmt.Println(str)
	}

	return
}
