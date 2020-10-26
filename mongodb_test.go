package resverKeeper

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMongodb(t *testing.T) {
	collName := "test_ver"
	identifier := "foo"

	db, err := NewMongodb("mongodb://localhost:27017", "test", collName)
	assert.Equal(t, nil, err)
	defer func() {
		dropTable(db, collName)
	}()

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
