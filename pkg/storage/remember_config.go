package storage

import (
	"context"
	"github.com/numary/ledger/pkg/config"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/logging"
)

type rememberConfigStorage struct {
	Store
	logger logging.Logger
}

func (s *rememberConfigStorage) AppendLog(ctx context.Context, log ...core.Log) (map[int]error, error) {
	defer config.Remember(ctx, s.logger, s.Name())
	return s.Store.AppendLog(ctx, log...)
}

func NewRememberConfigStorage(underlying Store, logger logging.Logger) *rememberConfigStorage {
	return &rememberConfigStorage{
		Store:  underlying,
		logger: logger,
	}
}

type RememberConfigStorageFactory struct {
	Factory
	logger logging.Logger
}

func (f *RememberConfigStorageFactory) GetStore(name string) (Store, error) {
	store, err := f.Factory.GetStore(name)
	if err != nil {
		return nil, err
	}
	return NewRememberConfigStorage(store, f.logger), nil
}

func NewRememberConfigStorageFactory(underlying Factory) *RememberConfigStorageFactory {
	return &RememberConfigStorageFactory{
		Factory: underlying,
	}
}

var _ Factory = &RememberConfigStorageFactory{}
