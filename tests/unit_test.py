import requests


def test_api_key(api_key):
    assert api_key == "MOCK_KEY1234"


def test_channel_handle(channel_handle):
    assert channel_handle == "MOCKHANDLE"


def test_postgres_conn(mock_postgres_conn_vars):
    conn = mock_postgres_conn_vars
    assert conn.login == "mock_username"
    assert conn.password == "mock_password"
    assert conn.host == "mock_host"
    assert conn.port == 1234
    assert conn.schema == "schema_db"


def test_dags_integrity(dagbag):
    # test the errors:
    assert dagbag.import_errors == {}, f"import error found:{dagbag.import_errors}"
    print("==========")
    print(dagbag.import_errors)

    # test the ids of the dagd
    expected_dag_ids = ["produce_json", "update_db", "dataquality_checks"]
    loaded_dag_ids = list(dagbag.dags.keys())
    print("==========")
    print(dagbag.dags.keys())

    # test the dag size
    assert dagbag.size() == 3
    print("==========")
    print(dagbag.size())

    # test which task have each dag:

    expected_task_count = {
        "produce_json": 5,
        "update_db": 3,
        "dataquality_checks": 2,
    }
    for dag_id, dag in dagbag.dags.items():
        expected_count = expected_task_count[dag_id]
        actual_count = len(dag.tasks)

        assert (
            expected_count == actual_count
        ), f"DAG {dag_id} has {actual_count} tasks, expected {expected_count}."
        print(dag_id, len(dag.tasks))


# airflow@082101a57b92:/opt/airflow$ pytest -v tests/unit_test.py -k test_postgres_conn
