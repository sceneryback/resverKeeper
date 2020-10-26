package resverKeeper

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TODO: use lock
func TestNewResverKeeper(t *testing.T) {
	identifier := "test"
	res := make(chan int)

	var kp *ResverKeeper
	go func() {
		keeper, _ := NewResverKeeper(&ResverKeeperConfig{
			ResourceIdentifier: identifier,
			DBtype:             DBTypePostgres,
			DBUrl:              "postgres://postgres:123@localhost:5432/postgres?sslmode=disable",
			DatabaseName:       "postgres",
			VersionStoreName:   "test_ver",
		}, func() error {
			fmt.Println("node1 reloaded")
			return nil
		})
		kp = keeper
		keeper.IncreaseVersion()
	}()
	defer func() {
		dropTable(kp.db, "test_ver")
	}()

	go func() {
		NewResverKeeper(&ResverKeeperConfig{
			ResourceIdentifier: identifier,
			DBtype:             DBTypePostgres,
			DBUrl:              "postgres://postgres:123@localhost:5432/postgres?sslmode=disable",
			DatabaseName:       "postgres",
			VersionStoreName:   "test_ver",
		}, func() error {
			fmt.Println("node2 reloaded")
			res <- 2
			return nil
		})
	}()

	v := <-res
	assert.Equal(t, 2, v)
}
