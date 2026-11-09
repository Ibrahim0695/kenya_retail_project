# Deployment evidence

Captured on 2026-11-09 from the local Docker Compose deployment.

- [`test-results.txt`](./test-results.txt) records the unit-test command and
  result: 12 tests passed.
- [`docker-compose-ps.txt`](./docker-compose-ps.txt) shows the Airflow
  metadata database, scheduler, webserver, and Kafka healthy.
- [`airflow-dag-running.png`](./airflow-dag-running.png) shows the Airflow
  grid for `kenya_retail_daily_pipeline` with a manual run active.
- [`airflow-run.txt`](./airflow-run.txt) records the run and task states.

The DAG had been paused, so it was unpaused and manually triggered for this
verification. This evidence shows an in-progress run, not a successful
end-to-end completion.
