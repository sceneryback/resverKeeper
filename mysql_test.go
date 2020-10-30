package resverKeeper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMysql(t *testing.T) {
	tableName := "test_ver"
	identifier := "foo"

	db, err := NewMysql("root@tcp(localhost:3306)/test", tableName)
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
