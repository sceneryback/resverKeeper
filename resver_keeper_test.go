package resverKeeper

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func testKeeper(t *testing.T, cfg *ResverKeeperConfig) {
	var db DB

	var wg sync.WaitGroup

	// simulating a node updates its version
	wg.Add(1)
	go func() {
		var node1Updated = make(chan struct{})

		keeper, err := NewResverKeeper(cfg, func() error {
			fmt.Println("node1 reloaded")
			node1Updated <- struct{}{}
			return nil
		})
		assert.Equal(t, nil, err)
		assert.NotEqual(t, nil, keeper)

		if keeper.db != nil {
			db = keeper.db
		}
		defer func() {
			// wait for version changed
			time.Sleep(time.Second)
			assert.Equal(t, 2, keeper.version)

			keeper.Close()
			wg.Done()
		}()

		go keeper.IncreaseVersion()

		for i := 0; i < 2; i++ {
			<-node1Updated
		}
	}()

	// simulating a node responds to db version change
	wg.Add(1)
	go func() {
		var node2Updated = make(chan struct{})

		keeper, err := NewResverKeeper(cfg, func() error {
			fmt.Println("node2 reloaded")
			node2Updated <- struct{}{}
			return nil
		})
		assert.Equal(t, nil, err)
		assert.NotEqual(t, nil, keeper)

		if keeper.db != nil {
			db = keeper.db
		}
		defer func() {
			// wait for version changed
			time.Sleep(time.Second)
			assert.Equal(t, 2, keeper.version)

			keeper.Close()
			wg.Done()
		}()

		for i := 0; i < 2; i++ {
			<-node2Updated
		}
	}()

	wg.Wait()

	if db != nil {
		dropTable(db, "test_ver")
	}
}

func TestNewResverKeeper(t *testing.T) {
	identifier := "test"
	store := "test_ver"

	resCfgs := []*ResverKeeperConfig{
		&ResverKeeperConfig{
			ResourceIdentifier:  identifier,
			DBtype:              DBTypePostgres,
			DBUrl:               "postgres://postgres:123@localhost:5432/postgres?sslmode=disable",
			DatabaseName:        "postgres",
			VersionStoreName:    store,
			VersionCheckSeconds: 3,
		},
		&ResverKeeperConfig{
			ResourceIdentifier:  identifier,
			DBtype:              DBTypeMysql,
			DBUrl:               "root@tcp(localhost:3306)/test",
			DatabaseName:        "test",
			VersionStoreName:    store,
			VersionCheckSeconds: 3,
		},
		&ResverKeeperConfig{
			ResourceIdentifier:  identifier,
			DBtype:              DBTypeMongodb,
			DBUrl:               "mongodb://localhost:27017",
			DatabaseName:        "test",
			VersionStoreName:    store,
			VersionCheckSeconds: 3,
		},
	}

	for i := range resCfgs {
		testKeeper(t, resCfgs[i])
	}
}
