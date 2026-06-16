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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUpgradeToVer239CreatesNonTransactionalDMLCheckpointTable(t *testing.T) {
	store, dom := CreateStoreAndBootstrap(t)
	defer func() { require.NoError(t, store.Close()) }()
	defer dom.Close()

	se := CreateSessionAndSetID(t, store)
	MustExec(t, se, "select job_id, range_id, checkpoint, status from mysql.tidb_nontransactional_dml_checkpoint limit 0")

	MustExec(t, se, "drop table mysql.tidb_nontransactional_dml_checkpoint")
	upgradeToVer239(se, version227)
	MustExec(t, se, "select job_id, range_id, checkpoint, status from mysql.tidb_nontransactional_dml_checkpoint limit 0")
}
