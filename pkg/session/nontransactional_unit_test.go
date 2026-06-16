// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package session

import (
	"errors"
	"testing"

	"github.com/pingcap/tidb/pkg/kv"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/stretchr/testify/require"
)

func TestCheckRangeModeStatementShape(t *testing.T) {
	cases := []struct {
		sql        string
		errContain string
	}{
		{
			sql:        "batch on a limit 1 insert into t1 select * from t",
			errContain: "range mode supports DELETE and UPDATE only",
		},
		{
			sql:        "batch on t.a limit 1 delete t from t join t1 on t.a = t1.a",
			errContain: "range mode supports single-table statements only",
		},
		{
			sql: "batch on a limit 1 delete from t where a > 1",
		},
		{
			sql: "batch on a limit 1 update t set b = b + 1",
		},
		{
			sql:        "batch on a limit 1 update t set b = b + 1 where a <= @upper_bound",
			errContain: "range mode doesn't support user variables",
		},
		{
			sql:        "batch on a limit 1 update t set b = @@sql_mode",
			errContain: "range mode doesn't support user variables",
		},
		{
			sql:        "batch on a limit 1 update t set b = connection_id()",
			errContain: "session-local functions",
		},
		{
			sql:        "batch on a limit 1 update t set b = last_insert_id()",
			errContain: "session-local functions",
		},
	}

	for _, tt := range cases {
		t.Run(tt.sql, func(t *testing.T) {
			stmt := parseNonTransactionalDML(t, tt.sql)
			err := checkRangeModeStatementShape(stmt)
			if err == nil {
				err = checkRangeModeConstraint(stmt, nil, nil, nil, []*ast.TableSource{{}})
			}
			if tt.errContain != "" {
				require.ErrorContains(t, err, tt.errContain)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestNonTransactionalDMLRangeRetryableError(t *testing.T) {
	require.False(t, isNonTransactionalDMLRangeRetryableError(nil))
	require.False(t, isNonTransactionalDMLRangeRetryableError(errors.New("check constraint failed")))
	require.True(t, isNonTransactionalDMLRangeRetryableError(errors.New(nonTransactionalDMLRangeInjectedErrMsg)))
	require.True(t, isNonTransactionalDMLRangeRetryableError(kv.ErrTxnRetryable.GenWithStackByArgs()))
}

func TestBuildNonTransactionalDMLRangeChunkSQLAddsHandleBounds(t *testing.T) {
	stmt := parseNonTransactionalDML(t, "batch on a limit 2 update t set b = b + 1 where b < 10")
	rangeCtx := &nonTransactionalDMLRangeContext{
		stmt:              stmt,
		handleColumn:      stmt.ShardColumn,
		handleColumnType:  *types.NewFieldType(mysql.TypeLong),
		originalCondition: stmt.DMLStmt.WhereExpr(),
	}

	start := int64(3)
	sql, err := buildNonTransactionalDMLRangeChunkSQL(rangeCtx, &start, 7)
	require.NoError(t, err)
	require.Contains(t, sql, "UPDATE `t` SET `b`=(`b` + 1)")
	require.Contains(t, sql, "`a` > 3")
	require.Contains(t, sql, "`a` <= 7")
	require.Contains(t, sql, "`b` < 10")

	whereSQL, err := restoreWhereExpression(stmt.DMLStmt.WhereExpr())
	require.NoError(t, err)
	require.Equal(t, "(`b` < 10)", whereSQL)

	sql, err = buildNonTransactionalDMLRangeChunkSQL(rangeCtx, nil, 5)
	require.NoError(t, err)
	require.Contains(t, sql, "`a` <= 5")
	require.NotContains(t, sql, "`a` >")
	require.Contains(t, sql, "`b` < 10")
}

func parseNonTransactionalDML(t *testing.T, sql string) *ast.NonTransactionalDMLStmt {
	t.Helper()

	node, err := parser.New().ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	stmt, ok := node.(*ast.NonTransactionalDMLStmt)
	require.True(t, ok)
	return stmt
}
