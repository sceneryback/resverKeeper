package resverKeeper

import "errors"

type DBType string

const (
	DBTypeMongodb  DBType = "mongodb"
	DBTypePostgres DBType = "postgres"
	DBTypeMysql    DBType = "mysql"
)

var (
	ErrDBTypeNotSupported = errors.New("db type not supported")
)

type DB interface {
	CreateVersionStore(string) error
	InitializeVersion(string) (int, error)
	GetVersion(string) (int, error)
	IncreaseVersion(string) (int, error)
}

func NewDB(typ DBType, url, database, tableName string) (DB, error) {
	var db DB
	var err error

	switch typ {
	case DBTypeMongodb:
		db, err = NewMongodb(url, database, tableName)
	case DBTypePostgres:
		db, err = NewPostgres(url, tableName)
	case DBTypeMysql:
		db, err = NewMysql(url, tableName)
	default:
		return nil, ErrDBTypeNotSupported
	}

	return db, err
}
