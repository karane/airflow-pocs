import pytest
from airflow.models import DagBag
from airflow.models.taskinstance import TaskInstance
from datetime import datetime


@pytest.fixture
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_no_import_errors(dagbag):
    assert len(dagbag.import_errors) == 0, f"Import errors: {dagbag.import_errors}"


def test_add_dag_loaded(dagbag):
    dag_id = "add_dag"
    assert dag_id in dagbag.dags
    dag = dagbag.get_dag(dag_id)
    assert dag is not None
    assert len(dag.tasks) == 3  # start, add_task, end


def test_add_task_callable(dagbag):
    dag = dagbag.get_dag("add_dag")
    task = dag.get_task("add_task")

    result = task.python_callable(**task.op_kwargs)
    assert result == 5

