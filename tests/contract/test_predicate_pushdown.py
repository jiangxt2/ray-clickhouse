"""Execute the connector hooks through Ray's native public Dataset optimizer."""

from collections.abc import Iterable
from unittest.mock import patch

import pyarrow as pa
import pyarrow.compute as pc
import pytest
import ray
from contract.test_ray_compatibility import ray_runtime as ray_runtime
from ray.data.block import BlockMetadata
from ray.data.datasource import Datasource
from ray.data.datatype import DataType
from ray.data.expressions import col, udf

from ray_clickhouse._compat import (
    empty_projection,
    make_read_task,
    supports_predicate_pushdown,
)
from ray_clickhouse._models import ClickHouseConnection, QualifiedTable
from ray_clickhouse._schema import SchemaPlan, TargetColumn
from ray_clickhouse.datasource import ClickHouseDatasource, ClickHouseReadConfig

pytestmark = pytest.mark.ray


@udf(return_dtype=DataType.bool())
def _late_id(values):
    return pc.greater_equal(values, 17)


class _ContractSource(Datasource):
    """Use real connector hooks with deterministic data and discovery fixtures."""

    def __init__(self, source, observations):
        super().__init__()
        self.source = source
        self.observations = observations

    def supports_predicate_pushdown(self):
        return self.source.supports_predicate_pushdown()

    def get_current_predicate(self):
        return self.source.get_current_predicate()

    def apply_predicate(self, expr):
        translated = self.source.apply_predicate(expr)
        self.observations.append(translated is not self.source)
        return (
            self
            if translated is self.source
            else _ContractSource(translated, self.observations)
        )

    def estimate_inmemory_data_size(self):
        return None

    def supports_projection_pushdown(self):
        return self.source.supports_projection_pushdown()

    def get_projection_map(self):
        return self.source.get_projection_map()

    def apply_projection(self, projection_map):
        return _ContractSource(
            self.source.apply_projection(projection_map), self.observations
        )

    def get_read_tasks(self, parallelism, per_task_row_limit=None, data_context=None):
        del parallelism, data_context
        table = pa.table({"id": list(range(20)), "value": [None, 1, 2, 3] * 5})
        predicate = self.source.get_current_predicate()
        if predicate is not None:
            table = table.filter(predicate.to_pyarrow())
        projection = self.source.get_projection_map()
        if projection is not None:
            table = (
                table.select(list(projection))
                if projection
                else empty_projection(table)
            )

        def read() -> Iterable[pa.Table]:
            yield table

        return [
            make_read_task(
                read,
                BlockMetadata(None, None, None, None),
                self.source.arrow_schema,
                per_task_row_limit,
            )
        ]


@pytest.fixture
def native_dataset():
    columns = tuple(
        TargetColumn(name, "Nullable(Int64)", "", "", i + 1)
        for i, name in enumerate(("id", "value"))
    )
    plan = SchemaPlan(pa.schema([("id", pa.int64()), ("value", pa.int64())]), columns)
    source = ClickHouseDatasource(
        ClickHouseReadConfig(
            ClickHouseConnection("unused", "db"), QualifiedTable("db", "events")
        )
    )
    observed = []
    with (
        patch("ray_clickhouse.datasource.discover_schema", return_value=plan),
        patch("ray_clickhouse.datasource.discover_engine", return_value="MergeTree"),
        patch(
            "ray_clickhouse.datasource.discover_predicate_context", return_value=True
        ),
    ):
        yield (
            ray.data.read_datasource(
                _ContractSource(source, observed), override_num_blocks=1
            ),
            observed,
        )


def test_native_acceptance_chaining_aliases_and_original_branch(native_dataset):
    base, observed = native_dataset
    branch = base.filter(expr=col("id") >= 12).filter(expr=col("value").is_not_null())
    assert sorted(row["id"] for row in branch.take_all()) == [13, 14, 15, 17, 18, 19]
    assert base.count() == 20
    alias = base.rename_columns({"id": "renamed"}).filter(expr=col("renamed") == 7)
    assert alias.take_all() == [{"renamed": 7, "value": 3}]
    assert any(observed) is supports_predicate_pushdown()


def test_native_rejection_retains_whole_or_and_residual_before_limit(native_dataset):
    base, observed = native_dataset
    # IN is Arrow-convertible but outside the connector profile: a partial OR
    # acceptance would incorrectly remove rows 0 and 1.
    expr = (col("id") >= 18) | col("id").is_in([0, 1])
    assert sorted(row["id"] for row in base.filter(expr=expr).take_all()) == [
        0,
        1,
        18,
        19,
    ]
    assert not any(observed)
    # Ray splits this opaque UDF from the Arrow-convertible integer condition.
    # The accepted prefix must not move a raw read limit before the residual.
    branch = base.filter(expr=(col("id") >= 3) & _late_id(col("id"))).limit(2)
    rows = branch.take_all()
    assert len(rows) == 2 and all(row["id"] >= 17 for row in rows)
    assert any(observed) is supports_predicate_pushdown()


def test_ray_inherited_limit_filter_behavior_is_characterized_separately(
    native_dataset,
):
    base, _ = native_dataset
    if not supports_predicate_pushdown():
        # Verify older releases retain their read/filter behavior without
        # asserting the new-version optimizer's inherited limit rule.
        assert len(base.filter(expr=col("id") >= 17).take_all()) == 3
        return
    # Ray 2.58/2.59 move the expression before Limit even without connector
    # support. This explicitly records inherited behavior, not authored order.
    result = base.limit(2).filter(expr=col("id") >= 17).take_all()
    assert len(result) == 2 and all(row["id"] >= 17 for row in result)


@pytest.mark.parametrize("columns", [["value"], ["id"], []])
def test_native_projection_and_predicate_composition(native_dataset, columns):
    base, _ = native_dataset
    reference = (
        ray.data.from_arrow(
            pa.table({"id": list(range(20)), "value": [None, 1, 2, 3] * 5})
        )
        .filter(expr=col("id") >= 12)
        .select_columns(columns)
        .materialize()
    )
    for branch in (
        base.filter(expr=col("id") >= 12).select_columns(columns),
        base.select_columns(["value", "id"])
        .filter(expr=col("id") >= 12)
        .select_columns(columns),
    ):
        result = branch.materialize()
        assert result.take_all() == reference.take_all()
        assert result.count() == reference.count()
        assert result.schema().names == reference.schema().names
    assert base.count() == 20
