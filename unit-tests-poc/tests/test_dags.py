import pytest
from airflow.models import DagBag


@pytest.fixture
def dagbag():
    return DagBag(dag_folder="dags", include_examples=False)


def test_no_import_errors(dagbag):
    """Ensure DAGs load without errors"""
    assert len(dagbag.import_errors) == 0, f"Import errors: {dagbag.import_errors}"


def test_example_dag_loaded(dagbag):
    """Check that example_dag exists"""
    dag_id = "example_dag"
    assert dag_id in dagbag.dags
    dag = dagbag.get_dag(dag_id)
    assert dag is not None
    assert len(dag.tasks) == 2


def test_example_dag_structure(dagbag):
    """Verify start -> end dependency"""
    dag = dagbag.get_dag("example_dag")
    task_dict = {t.task_id: t for t in dag.tasks}

    assert "start" in task_dict
    assert "end" in task_dict

    # Check downstream tasks
    assert task_dict["start"].downstream_task_ids == {"end"}
    assert task_dict["end"].downstream_task_ids == set()
