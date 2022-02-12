package sqlstorage

import (
	"context"
	"database/sql"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/sirupsen/logrus"
	"math"
	"time"
)

func (s *Store) FindTransactions(ctx context.Context, q query.Query) (query.Cursor, error) {
	q.Limit = int(math.Max(-1, math.Min(float64(q.Limit), 100))) + 1

	c := query.Cursor{}

	sb := sqlbuilder.NewSelectBuilder()
	sb.Distinct()
	sb.OrderBy("t.id desc")
	sb.Select("t.id", "t.timestamp", "t.reference", "t.metadata", "t.postings")
	switch s.flavor {
	case sqlbuilder.PostgreSQL:
		sb.From(s.table("transactions")+" t", "jsonb_to_recordset(t.postings) as postings(source varchar, destination varchar, asset varchar, amount bigint)")
	case sqlbuilder.SQLite:
		sb.From("transactions t", "json_each(postings)")
	}
	if q.After != "" {
		sb.Where(sb.LessThan("t.id", q.After))
	}
	sb.Limit(q.Limit)
	if q.HasParam("account") {
		switch s.flavor {
		case sqlbuilder.PostgreSQL:
			sb.Where(sb.Or(
				sb.Equal("source", q.Params["account"]),
				sb.Equal("destination", q.Params["account"]),
			))
		case sqlbuilder.SQLite:
			sb.Where(sb.Or(
				sb.Equal("json_extract(json_each.value, '$.source')", q.Params["account"]),
				sb.Equal("json_extract(json_each.value, '$.destination')", q.Params["account"]),
			))
		}

	}
	if q.HasParam("reference") {
		sb.Where(sb.E("reference", q.Params["reference"]))
	}
	sqlq, args := sb.BuildWithFlavor(s.flavor)
	logrus.Debugln(sqlq, args)
	rows, err := s.db.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return c, s.error(err)
	}

	transactions := make([]core.Transaction, 0)

	for rows.Next() {
		var (
			ref sql.NullString
			ts  sql.NullString
		)

		tx := core.Transaction{}
		err := rows.Scan(
			&tx.ID,
			&ts,
			&ref,
			&tx.Metadata,
			&tx.Postings,
		)
		if err != nil {
			return c, err
		}
		tx.Reference = ref.String
		if tx.Metadata == nil {
			tx.Metadata = core.Metadata{}
		}
		timestamp, err := time.Parse(time.RFC3339, ts.String)
		if err != nil {
			return query.Cursor{}, err
		}
		tx.Timestamp = timestamp
		transactions = append(transactions, tx)
	}
	if rows.Err() != nil {
		return query.Cursor{}, s.error(err)
	}

	c.PageSize = q.Limit - 1
	c.HasMore = len(transactions) == q.Limit
	if c.HasMore {
		transactions = transactions[:len(transactions)-1]
	}
	c.Data = transactions

	// TODO: The count should match the query
	total, _ := s.CountTransactions(ctx)
	c.Total = total

	return c, nil
}

func (s *Store) GetTransaction(ctx context.Context, txid string) (tx core.Transaction, err error) {
	sb := sqlbuilder.NewSelectBuilder()
	sb.Select(
		"t.id",
		"t.timestamp",
		"t.reference",
		"t.metadata",
		"t.postings",
	)
	sb.From(sb.As(s.table("transactions"), "t"))
	sb.Where(sb.Equal("t.id", txid))

	sqlq, args := sb.BuildWithFlavor(s.flavor)
	rows, err := s.db.QueryContext(ctx, sqlq, args...)
	if err != nil {
		return tx, s.error(err)
	}

	for rows.Next() {
		var (
			ref sql.NullString
			ts  sql.NullString
		)

		err := rows.Scan(
			&tx.ID,
			&ts,
			&ref,
			&tx.Metadata,
			&tx.Postings,
		)
		if err != nil {
			return tx, err
		}

		if tx.Metadata == nil {
			tx.Metadata = core.Metadata{}
		}
		t, err := time.Parse(time.RFC3339, ts.String)
		if err != nil {
			return tx, err
		}
		tx.Timestamp = t
		tx.Reference = ref.String
	}
	if rows.Err() != nil {
		return tx, s.error(rows.Err())
	}

	return tx, nil
}
