package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/razmser/ch-exp/data_model"
)

func createTablesCh(ctx context.Context, conn *ch.Client) {
	if err := conn.Do(ctx, ch.Query{
		Body: `CREATE TABLE IF NOT EXISTS exp
(
    id Int32,
    value AggregateFunction(uniq, Int32)
)
ENGINE = MergeTree
ORDER BY id`,
	}); err != nil {
		panic(err)
	}

	if err := conn.Do(ctx, ch.Query{
		Body: `CREATE TABLE IF NOT EXISTS exp_input
(
    id Int32,
    value Int32
)
ENGINE = Null`,
	}); err != nil {
		panic(err)
	}

	if err := conn.Do(ctx, ch.Query{
		Body: `CREATE MATERIALIZED VIEW IF NOT EXISTS exp_mv TO exp AS
SELECT id, uniqState(value) AS value
FROM exp_input
GROUP BY id`,
	}); err != nil {
		panic(err)
	}
}

func insertCh(ctx context.Context, conn *ch.Client) {
	// Define all columns of table.
	var (
		id    proto.ColInt32
		value proto.ColInt32
	)

	// Append 10 rows to initial data block.
	id.Append(1)
	value.Append(1)
	id.Append(1)
	value.Append(2)
	id.Append(2)
	value.Append(2)
	id.Append(2)
	value.Append(3)

	// Insert single data block.
	input := proto.Input{
		{Name: "id", Data: id},
		{Name: "value", Data: value},
	}
	if err := conn.Do(ctx, ch.Query{
		Body: "INSERT INTO exp_input VALUES",
		// Or "INSERT INTO test_table_insert (ts, severity_text, severity_number, body, name, arr) VALUES"
		Input: input,
	}); err != nil {
		panic(err)
	}
}

func selectCh(ctx context.Context, conn *ch.Client) {
	var (
		id    proto.ColInt32
		value proto.ColUInt64
	)
	if err := conn.Do(ctx, ch.Query{
		Body: "SELECT id, uniqMerge(value) AS value FROM exp GROUP BY id",
		Result: proto.Results{
			{Name: "id", Data: &id},
			{Name: "value", Data: &value},
		},
	}); err != nil {
		panic(err)
	}
	fmt.Println("id\tvalue")
	for i := range id {
		fmt.Printf("%d\t%d\n", id[i], value[i])
	}
}

func selectAggStateCh(ctx context.Context, conn *ch.Client) {
	var (
		id    proto.ColInt32
		value data_model.AggregateUniqInt32
	)
	if err := conn.Do(ctx, ch.Query{
		Body: "SELECT id, uniqMergeState(value) AS value FROM exp GROUP BY id",
		Result: proto.Results{
			{Name: "id", Data: &id},
			{Name: "value", Data: &value},
		},
	}); err != nil {
		panic(err)
	}

	fmt.Println("id\tvalue")
	for i := range id {
		fmt.Printf("%d\t%d\n", id[i], value[i].ItemsCount())
	}
	value[0].Merge(value[1])
	fmt.Println("merged items count", value[0].ItemsCount())
}

func main() {
	ctx := context.Background()

	slog.Info("connecting...")
	conn, err := ch.Dial(ctx, ch.Options{})
	if err != nil {
		panic(err)
	}
	slog.Info("connected")

	slog.Info("creating table...")
	createTablesCh(ctx, conn)
	slog.Info("table created")

	slog.Info("inserting...")
	insertCh(ctx, conn)
	slog.Info("inserted")

	slog.Info("selecting...")
	selectCh(ctx, conn)
	slog.Info("selected")

	slog.Info("selecting agg...")
	selectAggStateCh(ctx, conn)
	slog.Info("selected agg")
}
