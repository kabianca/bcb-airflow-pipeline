"""DAG integrity tests.

Cheap, high-signal: catches import errors, missing retries and accidental
`catchup=False` before they reach the scheduler. Skipped automatically when
Airflow is not installed, so `pytest` still runs the pure-python suite in a
bare virtualenv.
"""

from __future__ import annotations

import pytest

from importlib.metadata import PackageNotFoundError, version  # noqa: E402

try:  # the task SDK alone provides a partial `airflow` namespace - check core
    version("apache-airflow")
except PackageNotFoundError:  # pragma: no cover
    pytest.skip("apache-airflow core not installed", allow_module_level=True)

from airflow.models import DagBag  # noqa: E402

EXPECTED_DAG_IDS = {"bcb_series_ingest"}


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder="dags")


def test_no_import_errors(dagbag):
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_expected_dags_are_present(dagbag):
    assert EXPECTED_DAG_IDS.issubset(set(dagbag.dags))


def test_every_task_has_retries(dagbag):
    for dag_id, dag in dagbag.dags.items():
        for task in dag.tasks:
            assert task.retries >= 1, f"{dag_id}.{task.task_id} has no retries"


def test_every_dag_has_tags_and_owner_docs(dagbag):
    for dag_id, dag in dagbag.dags.items():
        assert dag.tags, f"{dag_id} has no tags"
        assert dag.doc_md, f"{dag_id} has no doc_md"


def test_ingest_dag_backfills(dagbag):
    """catchup=True is load-bearing here - Airflow 3 defaults it to False."""
    dag = dagbag.dags["bcb_series_ingest"]
    assert dag.catchup is True
    assert dag.max_active_runs and dag.max_active_runs <= 5


def test_no_cycles(dagbag):
    for dag in dagbag.dags.values():
        dag.test_cycle() if hasattr(dag, "test_cycle") else dag.validate()
