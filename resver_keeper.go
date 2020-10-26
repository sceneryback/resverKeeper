/*
Resource Version Keeper
*/
package resverKeeper

import (
	"errors"
	"go.uber.org/zap"
	"sync"
	"time"
)

const (
	DefaultVersionCheckSeconds = 10
	DefaultVersionStoreName    = "resver"
)

var (
	ErrConfigIdentifierRequired = errors.New("resource identifier required")
	ErrReloadingFuncRequired    = errors.New("reloading function required")
	ErrDBTypeUrlRequired        = errors.New("database type and url required")

	logger *zap.SugaredLogger
)

func init() {
	log, _ := zap.NewDevelopment()
	logger = log.Sugar()
}

type ResverKeeperConfig struct {
	ResourceIdentifier  string
	DBtype              DBType
	DBUrl               string
	DatabaseName        string
	VersionStoreName    string
	VersionCheckSeconds int
}

type ResverKeeper struct {
	cfg *ResverKeeperConfig

	version int
	mu      sync.RWMutex

	db DB

	reload func() error

	ticker *time.Ticker
}

func NewResverKeeper(cfg *ResverKeeperConfig, reload func() error) (*ResverKeeper, error) {
	if cfg == nil || cfg.ResourceIdentifier == "" {
		return nil, ErrConfigIdentifierRequired
	}

	if reload == nil {
		return nil, ErrReloadingFuncRequired
	}

	if cfg.DBtype == "" || cfg.DBUrl == "" {
		return nil, ErrDBTypeUrlRequired
	}

	if cfg.VersionCheckSeconds == 0 {
		cfg.VersionCheckSeconds = DefaultVersionCheckSeconds
	}

	if cfg.VersionStoreName == "" {
		cfg.VersionStoreName = DefaultVersionStoreName
	}

	db, err := NewDB(cfg.DBtype, cfg.DBUrl, cfg.DatabaseName, cfg.VersionStoreName)
	if err != nil {
		logger.Errorw("failed to new db", "type", cfg.DBtype, "url", cfg.DBUrl)
		return nil, err
	}

	var keeper = &ResverKeeper{
		cfg:    cfg,
		reload: reload,
		db:     db,
	}

	err = keeper.initialize()
	if err != nil {
		logger.Errorw("failed to initialize", "err", err.Error())
		//return nil, err
	}

	go keeper.startWatching()

	return keeper, nil
}

func (r *ResverKeeper) initialize() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	v, err := r.db.GetVersion(r.cfg.ResourceIdentifier)
	if err != nil {
		v1, err1 := r.db.InitializeVersion(r.cfg.ResourceIdentifier)
		if err1 != nil {
			logger.Errorf("failed to initialize version", "identifier", r.cfg.ResourceIdentifier, "err", err1.Error())
			return err1
		}
		r.version = v1
	} else {
		r.version = v
	}
	return nil
}

func (r *ResverKeeper) startWatching() {
	err := r.reload()
	if err != nil {
		logger.Errorw("failed to reload", "err", err.Error())
		return
	}

	go func() {
		ticker := time.NewTicker(time.Duration(r.cfg.VersionCheckSeconds) * time.Second)
		r.ticker = ticker
		var err error
		var v int
		for range ticker.C {
			v, err = r.db.GetVersion(r.cfg.ResourceIdentifier)
			if err != nil {
				logger.Errorw("failed to get version", "err", err.Error())
				continue
			}
			if v == r.version {
				continue
			}
			err = r.reload()
			if err != nil {
				logger.Errorw("failed to reload", "err", err.Error())
				continue
			}
			// reset version finally
			r.mu.Lock()
			r.version = v
			r.mu.Unlock()
		}
	}()
}

func (r *ResverKeeper) IncreaseVersion() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	v, err := r.db.IncreaseVersion(r.cfg.ResourceIdentifier)
	if err != nil {
		return err
	}
	r.version = v
	return nil
}

func (r *ResverKeeper) Close() {
	if r.ticker != nil {
		r.ticker.Stop()
	}
}
