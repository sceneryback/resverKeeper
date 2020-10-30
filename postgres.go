package resverKeeper

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"sync"
)

var pgOnce sync.Once

type Postgres struct {
	pool      *pgxpool.Pool
	tableName string
}

func NewPostgres(url, tableName string) (DB, error) {
	var db Postgres

	dbpool, err := pgxpool.Connect(context.Background(), url)
	if err != nil {
		logger.Errorw("Unable to connect to database", "url", url, "err", err.Error())
		return nil, err
	}
	db.pool = dbpool

	db.tableName = tableName
	pgOnce.Do(func() {
		err = db.CreateVersionStore(tableName)
		if err != nil {
			logger.Errorw("failed to create version store", "err", err.Error())
		}
	})

	return &db, err
}

func (p *Postgres) CreateVersionStore(storeName string) error {
	sql := fmt.Sprintf(`CREATE TABLE if not exists %s (
		id bigserial,
		identifier varchar(128),
		version bigint,
		PRIMARY KEY(identifier)
	);
	CREATE INDEX if not exists resource_ver_identifier_index ON %s (identifier);
	`, p.tableName, p.tableName)

	_, err := p.pool.Exec(context.Background(), sql)
	if err != nil {
		return err
	}

	return nil
}

func (p *Postgres) InitializeVersion(identifier string) (int, error) {
	sql := fmt.Sprintf(`
		insert into %s (identifier, version) values ('%s', %d) on conflict(identifier) do nothing
	`, p.tableName, identifier, 1)
	_, err := p.pool.Exec(context.Background(), sql)
	if err != nil {
		return 0, err
	}

	return 1, nil
}

func (p *Postgres) GetVersion(identifier string) (int, error) {
	sql := fmt.Sprintf(`select version from %s where identifier = '%s'`, p.tableName, identifier)
	var version int
	err := p.pool.QueryRow(context.Background(), sql).Scan(&version)
	if err != nil {
		return 0, err
	}
	return version, nil
}

func (p *Postgres) IncreaseVersion(identifier string) (int, error) {
	sql := fmt.Sprintf(`
		update %s set version = version + 1 where identifier = '%s'
	`, p.tableName, identifier)
	_, err := p.pool.Exec(context.Background(), sql)
	if err != nil {
		return 0, err
	}

	return p.GetVersion(identifier)
}
