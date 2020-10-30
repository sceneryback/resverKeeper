package resverKeeper

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func dropTable(db DB, table string) {
	switch t := db.(type) {
	case *Postgres:
		t.pool.Exec(context.Background(), "drop table "+table)
	case *Mysql:
		t.db.Exec("drop table " + table)
	case *Mongodb:
		t.coll.Drop(context.TODO())
	}
}

func TestPostgres(t *testing.T) {
	tableName := "test_ver"
	identifier := "foo"

	db, err := NewPostgres("postgres://postgres:123@localhost:5432/postgres?sslmode=disable", tableName)
	assert.Equal(t, nil, err)
	defer func() {
		dropTable(db, tableName)
	}()

	err = db.CreateVersionStore(tableName)
	assert.Equal(t, nil, err)

	v, err := db.InitializeVersion(identifier)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, v)

	v, err = db.GetVersion(identifier)
	assert.Equal(t, nil, err)
	assert.Equal(t, 1, v)

	v, err = db.IncreaseVersion(identifier)
	assert.Equal(t, nil, err)
	assert.Equal(t, 2, v)
}
