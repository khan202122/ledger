package sqlstorage_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/davecgh/go-spew/spew"
	"github.com/numary/ledger/internal/pgtesting"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/logging"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"os"
	"testing"
	"time"
)

func TestStore(t *testing.T) {

	if testing.Verbose() {
		logrus.StandardLogger().Level = logrus.DebugLevel
	}

	type testingFunction struct {
		name string
		fn   func(t *testing.T, store *sqlstorage.Store)
	}

	for _, tf := range []testingFunction{
		{
			name: "SaveTransactions",
			fn:   testSaveTransaction,
		},
		{
			name: "DuplicatedTransaction",
			fn:   testDuplicatedTransaction,
		},
		{
			name: "LastLog",
			fn:   testLastLog,
		},
		{
			name: "CountAccounts",
			fn:   testCountAccounts,
		},
		{
			name: "AggregateBalances",
			fn:   testAggregateBalances,
		},
		{
			name: "AggregateVolumes",
			fn:   testAggregateVolumes,
		},
		{
			name: "FindAccounts",
			fn:   testFindAccounts,
		},
		{
			name: "CountTransactions",
			fn:   testCountTransactions,
		},
		{
			name: "FindTransactions",
			fn:   testFindTransactions,
		},
		{
			name: "GetTransaction",
			fn:   testGetTransaction,
		},
		{
			name: "Mapping",
			fn:   testMapping,
		},
	} {
		t.Run(fmt.Sprintf("%s/%s", ledgertesting.StorageDriverName(), tf.name), func(t *testing.T) {

			done := make(chan struct{})
			app := fx.New(
				ledgertesting.StorageModule(),
				fx.Provide(storage.NewDefaultFactory),
				logging.LogrusModule(),
				fx.Invoke(func(storageFactory storage.Factory, lc fx.Lifecycle) {
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							defer func() {
								close(done)
							}()
							ledger := uuid.New()
							store, err := storageFactory.GetStore(ledger)
							assert.NoError(t, err)

							assert.NoError(t, err)
							defer store.Close(context.Background())

							err = store.Initialize(context.Background())
							assert.NoError(t, err)

							tf.fn(t, store.(*sqlstorage.Store))
							return nil
						},
					})
				}),
			)
			go app.Start(context.Background())
			defer app.Stop(context.Background())

			select {
			case <-time.After(5 * time.Second):
			case <-done:
			}
		})
	}

}

func testSaveTransaction(t *testing.T, store *sqlstorage.Store) {
	_, err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, core.Transaction{
		ID: uuid.New(),
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
		},
		Timestamp: time.Now().Round(time.Second),
	}))
	assert.NoError(t, err)
}

func testDuplicatedTransaction(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: uuid.New(),
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{},
			},
			Reference: "foo",
		},
	}
	log := core.NewTransactionLog(nil, tx)
	_, err := store.AppendLog(context.Background(), log)
	assert.NoError(t, err)

	tx.ID = uuid.New()
	ret, err := store.AppendLog(context.Background(), core.NewTransactionLog(&log, tx))
	assert.Error(t, err)
	assert.Equal(t, storage.ErrAborted, err)
	assert.Len(t, ret, 1)
	assert.IsType(t, &storage.Error{}, ret[0])
	assert.Equal(t, storage.ConstraintFailed, ret[0].(*storage.Error).Code)
}

func testCountAccounts(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: uuid.New(),
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
		},
		Timestamp: time.Now().Round(time.Second),
	}
	_, err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
	assert.NoError(t, err)

	countAccounts, err := store.CountAccounts(context.Background())
	assert.EqualValues(t, 2, countAccounts) // world + central_bank

}

func testAggregateBalances(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: uuid.New(),
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
		},
		Timestamp: time.Now().Round(time.Second),
	}
	_, err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
	assert.NoError(t, err)

	balances, err := store.AggregateBalances(context.Background(), "central_bank")
	assert.NoError(t, err)
	assert.Len(t, balances, 1)
	assert.EqualValues(t, 100, balances["USD"])
}

func testAggregateVolumes(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: uuid.New(),
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
		},
		Timestamp: time.Now().Round(time.Second),
	}
	_, err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
	assert.NoError(t, err)

	volumes, err := store.AggregateVolumes(context.Background(), "central_bank")
	assert.NoError(t, err)
	assert.Len(t, volumes, 1)
	assert.Len(t, volumes["USD"], 2)
	assert.EqualValues(t, 100, volumes["USD"]["input"])
	assert.EqualValues(t, 0, volumes["USD"]["output"])
}

func testFindAccounts(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: uuid.New(),
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
		},
		Timestamp: time.Now().Round(time.Second),
	}
	_, err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
	assert.NoError(t, err)

	accounts, err := store.FindAccounts(context.Background(), query.Query{
		Limit: 1,
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 2, accounts.Total)
	assert.True(t, accounts.HasMore)
	assert.Equal(t, 1, accounts.PageSize)

	accounts, err = store.FindAccounts(context.Background(), query.Query{
		Limit: 1,
		After: accounts.Data.([]core.Account)[0].Address,
	})
	assert.NoError(t, err)
	assert.EqualValues(t, 2, accounts.Total)
	assert.False(t, accounts.HasMore)
	assert.Equal(t, 1, accounts.PageSize)
}

func testCountTransactions(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: uuid.New(),
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Metadata: map[string]json.RawMessage{
				"lastname": json.RawMessage(`"XXX"`),
			},
		},
		Timestamp: time.Now().Round(time.Second),
	}
	_, err := store.AppendLog(context.Background(), core.NewTransactionLog(nil, tx))
	assert.NoError(t, err)

	countTransactions, err := store.CountTransactions(context.Background())
	assert.NoError(t, err)
	assert.EqualValues(t, 1, countTransactions)
}

func testFindTransactions(t *testing.T, store *sqlstorage.Store) {
	tx1 := core.Transaction{
		ID: uuid.New(),
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Reference: "tx1",
		},
		Timestamp: time.Now().Round(time.Second),
	}
	tx2 := core.Transaction{
		ID: uuid.New(),
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Reference: "tx2",
		},
		Timestamp: time.Now().Round(time.Second),
	}
	log1 := core.NewTransactionLog(nil, tx1)
	log2 := core.NewTransactionLog(&log1, tx2)
	_, err := store.AppendLog(context.Background(), log1, log2)
	assert.NoError(t, err)

	cursor, err := store.FindTransactions(context.Background(), query.Query{
		Limit: 1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, cursor.PageSize)
	assert.True(t, cursor.HasMore)

	cursor, err = store.FindTransactions(context.Background(), query.Query{
		After: fmt.Sprint(cursor.Data.([]core.Transaction)[0].ID),
		Limit: 1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, cursor.PageSize)
	assert.False(t, cursor.HasMore)

	cursor, err = store.FindTransactions(context.Background(), query.Query{
		Params: map[string]interface{}{
			"account":   "world",
			"reference": "tx1",
		},
		Limit: 1,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, cursor.PageSize)
	assert.False(t, cursor.HasMore)

}

func testMapping(t *testing.T, store *sqlstorage.Store) {

	m := core.Mapping{
		Contracts: []core.Contract{
			{
				Expr: &core.ExprGt{
					Op1: core.VariableExpr{Name: "balance"},
					Op2: core.ConstantExpr{Value: float64(0)},
				},
				Account: "orders:*",
			},
		},
	}
	err := store.SaveMapping(context.Background(), m)
	assert.NoError(t, err)

	mapping, err := store.LoadMapping(context.Background())
	assert.NoError(t, err)
	assert.Len(t, mapping.Contracts, 1)
	assert.EqualValues(t, m.Contracts[0], mapping.Contracts[0])

	m2 := core.Mapping{
		Contracts: []core.Contract{},
	}
	err = store.SaveMapping(context.Background(), m2)
	assert.NoError(t, err)

	mapping, err = store.LoadMapping(context.Background())
	assert.NoError(t, err)
	assert.Len(t, mapping.Contracts, 0)
}

func testGetTransaction(t *testing.T, store *sqlstorage.Store) {
	tx1 := core.Transaction{
		ID: uuid.New(),
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Reference: "tx1",
			Metadata:  map[string]json.RawMessage{},
		},
		Timestamp: time.Now().Round(time.Second),
	}
	tx2 := core.Transaction{
		ID: uuid.New(),
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
			Reference: "tx2",
			Metadata:  map[string]json.RawMessage{},
		},
		Timestamp: time.Now().Round(time.Second),
	}
	log1 := core.NewTransactionLog(nil, tx1)
	log2 := core.NewTransactionLog(&log1, tx2)
	_, err := store.AppendLog(context.Background(), log1, log2)
	assert.NoError(t, err)

	tx, err := store.GetTransaction(context.Background(), tx1.ID)
	assert.NoError(t, err)
	assert.Equal(t, tx1, tx)
}

func testTooManyClient(t *testing.T, store *sqlstorage.Store) {

	if os.Getenv("NUMARY_STORAGE_POSTGRES_CONN_STRING") != "" { // Use of external server, ignore this test
		return
	}

	for i := 0; i < pgtesting.MaxConnections; i++ {
		tx, err := store.DB().BeginTx(context.Background(), nil)
		assert.NoError(t, err)
		defer tx.Rollback()
	}
	_, err := store.CountTransactions(context.Background())
	assert.Error(t, err)
	assert.IsType(t, new(storage.Error), err)
	assert.Equal(t, storage.TooManyClient, err.(*storage.Error).Code)
}

func testLastLog(t *testing.T, store *sqlstorage.Store) {
	tx := core.Transaction{
		ID: uuid.New(),
		TransactionData: core.TransactionData{
			Postings: []core.Posting{
				{
					Source:      "world",
					Destination: "central_bank",
					Amount:      100,
					Asset:       "USD",
				},
			},
		},
		Timestamp: time.Now().Round(time.Second),
	}
	log := core.NewTransactionLog(nil, tx)
	_, err := store.AppendLog(context.Background(), log)
	assert.NoError(t, err)

	lastLog, err := store.LastLog(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, lastLog)
	spew.Dump(err)
	assert.EqualValues(t, tx, lastLog.Data)
}
