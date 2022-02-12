package sqlstorage

import (
	"context"
	"github.com/huandu/go-sqlbuilder"
)

func (s *Store) countTransactions(ctx context.Context, exec Executor) (int64, error) {
	var count int64

	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("count(*)")
	sb.From(s.table("transactions"))

	sqlq, args := sb.Build()

	err := exec.QueryRowContext(ctx, sqlq, args...).Scan(&count)

	return count, s.error(err)
}

func (s *Store) CountTransactions(ctx context.Context) (int64, error) {
	return s.countTransactions(ctx, s.db)
}

func (s *Store) aggregateBalances(ctx context.Context, exec Executor, address string) (map[string]int64, error) {

	sb := sqlbuilder.NewSelectBuilder()
	sb.From(s.table("balances"))
	sb.Select("asset", "amount")
	sb.Where(sb.Equal("account", address))

	sql, args := sb.BuildWithFlavor(s.flavor)
	rows, err := exec.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, s.error(err)
	}

	balances := make(map[string]int64)
	for rows.Next() {
		var (
			asset  string
			amount int64
		)
		err = rows.Scan(&asset, &amount)
		if err != nil {
			return nil, err
		}
		balances[asset] = amount
	}
	return balances, nil
}

func (s *Store) AggregateBalances(ctx context.Context, address string) (map[string]int64, error) {
	return s.aggregateBalances(ctx, s.db, address)
}

func (s *Store) aggregateVolumes(ctx context.Context, exec Executor, address string) (map[string]map[string]int64, error) {

	sb := sqlbuilder.NewSelectBuilder()
	sb.Select("asset", "input", "output")
	sb.From(s.table("volumes"))
	sb.Where(sb.E("account", address))

	sql, args := sb.BuildWithFlavor(s.flavor)
	rows, err := exec.QueryContext(ctx, sql, args...)
	if err != nil {
		return nil, s.error(err)
	}

	volumes := make(map[string]map[string]int64)
	for rows.Next() {
		var (
			asset  string
			input  int64
			output int64
		)
		err = rows.Scan(&asset, &input, &output)
		if err != nil {
			return nil, s.error(err)
		}
		volumes[asset] = map[string]int64{
			"input":  input,
			"output": output,
		}
	}
	if err := rows.Err(); err != nil {
		return nil, s.error(err)
	}

	return volumes, nil
}

func (s *Store) AggregateVolumes(ctx context.Context, address string) (map[string]map[string]int64, error) {
	return s.aggregateVolumes(ctx, s.db, address)
}
