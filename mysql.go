package resverKeeper

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
)

type Mysql struct {
	tableName string
	db        *sql.DB
}

func NewMysql(url, tableName string) (*Mysql, error) {
	var mysql Mysql

	db, err := sql.Open("mysql", url)
	if err != nil {
		log.Printf("failed to open sql: %s", err)
		return nil, err
	}
	mysql.db = db

	mysql.tableName = tableName
	err = mysql.CreateVersionStore(tableName)
	if err != nil {
		return nil, err
	}

	return &mysql, nil
}

func (m *Mysql) CreateVersionStore(storeName string) error {
	sql := fmt.Sprintf(`CREATE TABLE if not exists %s (
		id bigint PRIMARY KEY AUTO_INCREMENT,
		identifier varchar(128) UNIQUE,
		version bigint,
		INDEX(identifier)
	);`, m.tableName)

	_, err := m.db.Exec(sql)
	if err != nil {
		log.Printf("failed to create version store: %s", err)
		return err
	}

	return nil
}

func (m *Mysql) InitializeVersion(identifier string) (int, error) {
	sql := fmt.Sprintf(`
		insert into %s (identifier, version) values ('%s', %d)
	`, m.tableName, identifier, 1)
	_, err := m.db.Exec(sql)
	if err != nil {
		return 0, err
	}

	return 1, nil
}

func (m *Mysql) GetVersion(identifier string) (int, error) {
	sql := fmt.Sprintf(`
		select version from %s where identifier = '%s'
	`, m.tableName, identifier)
	var version int
	err := m.db.QueryRow(sql).Scan(&version)
	if err != nil {
		log.Printf("failed to query version for %s: %s", identifier, err)
		return 0, err
	}
	return version, nil
}

func (m *Mysql) IncreaseVersion(identifier string) (int, error) {
	sql := fmt.Sprintf(`
		update %s set version = version + 1 where identifier = '%s'
	`, m.tableName, identifier)
	_, err := m.db.Exec(sql)
	if err != nil {
		return 0, err
	}

	return m.GetVersion(identifier)
}
