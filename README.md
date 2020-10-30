# resverKeeper
A Resource Version Keeper that can help you keep the version of some resource consistent within your clusters.

## How to use

Suppose you want to use ```resverKeeper``` to keep the version of your users database and you have a cache which should be consistent with your database.

1. Start the keeper

```go
keeper, err := resverKeeper.NewResverKeeper(&ResverKeeperConfig{
    ResourceIdentifier:  "users",
    DBtype:              resverKeeper.DBTypeMysql,
    DBUrl:               "root@tcp(localhost:3306)/test",
    DatabaseName:        "test",
    VersionStoreName:    "res_ver",
    VersionCheckSeconds: 10,
  }, func() error {
    // read users data from database and build your cache
    cacheUsersFromDatabase()
    return nil
})
```

2. Whenever any node server of your cluster does CREATE/UPDATE/DELETE operations, update the version

```go
keeper.IncreaseVersion()
```

3. All nodes will receive version changes and rebuild its cache

```go
// resverKeeper calls reload function
cacheUsersFromDatabase()
```

So with ```resverKeeper``` you avoid fetching Database iteratively and keep the cache consistent with DB in a more economical way.

## Supported databases

Currently support the following:

* mysql
* postgres
* mongodb

