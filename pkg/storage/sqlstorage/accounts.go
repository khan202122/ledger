package sqlstorage

import (
	"context"
	"database/sql"
	"math"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
)

func (s *Store) findAccounts(ctx context.Context, exec Executor, q query.Query) (query.Cursor, error) {
	// We fetch an additional account to know if we have more documents
	q.Limit = int(math.Max(-1, math.Min(float64(q.Limit), 100))) + 1

	c := query.Cursor{}
	results := make([]core.Account, 0)

	sb := sqlbuilder.NewSelectBuilder()
	sb.
		Select("address", "metadata").
		From(s.table("accounts")).
		Limit(q.Limit)

	if q.After != "" {
		sb.Where(sb.LessThan("address", q.After))
	}

	sqlq, args := sb.BuildWithFlavor(s.flavor)
	rows, err := exec.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return c, s.error(err)
	}

	for rows.Next() {
		account := core.Account{}
		var (
			addr sql.NullString
			m    sql.NullString
		)
		err := rows.Scan(&addr, &m)
		if err != nil {
			return c, err
		}
		err = rows.Scan(&account.Address, &account.Metadata)
		if err != nil {
			return c, err
		}
		results = append(results, account)
	}
	if rows.Err() != nil {
		return c, rows.Err()
	}

	c.PageSize = q.Limit - 1

	c.HasMore = len(results) == q.Limit
	if c.HasMore {
		results = results[:len(results)-1]
	}
	c.Data = results

	total, _ := s.CountAccounts(ctx)
	c.Total = total

	return c, nil
}

func (s *Store) FindAccounts(ctx context.Context, q query.Query) (query.Cursor, error) {
	return s.findAccounts(ctx, s.db, q)
}

func (s *Store) getAccount(ctx context.Context, exec Executor, addr string) (core.Account, error) {

	sb := sqlbuilder.NewSelectBuilder()
	sb.
		Select("address", "metadata").
		From(s.table("accounts")).
		Where(sb.Equal("address", addr))

	sqlq, args := sb.BuildWithFlavor(s.flavor)
	row := exec.QueryRowContext(ctx, sqlq, args...)

	account := core.Account{}
	err := row.Scan(&account.Address, &account.Metadata)
	if err != nil {
		if err == sql.ErrNoRows {
			return core.Account{}, nil
		}
		return core.Account{}, err
	}

	return account, nil
}

func (s *Store) GetAccount(ctx context.Context, addr string) (core.Account, error) {
	return s.getAccount(ctx, s.db, addr)
}

func (s *Store) countAccounts(ctx context.Context, exec Executor) (int64, error) {
	var count int64

	sb := sqlbuilder.NewSelectBuilder()
	sb.
		Select("count(*)").
		From(s.table("accounts")).
		BuildWithFlavor(s.flavor)

	sqlq, args := sb.Build()

	err := exec.QueryRowContext(ctx, sqlq, args...).Scan(&count)

	return count, s.error(err)
}

func (s *Store) CountAccounts(ctx context.Context) (int64, error) {
	return s.countAccounts(ctx, s.db)
}
