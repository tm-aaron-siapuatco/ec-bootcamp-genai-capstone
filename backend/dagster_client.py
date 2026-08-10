import os
import time
import httpx

DAGSTER_GRAPHQL_URL = os.getenv("DAGSTER_GRAPHQL_URL", "http://localhost:3000/graphql")

# Use Dagster default names, since no job name specificed
REPOSITORY_LOCATION_NAME = "pipelines.definitions"
REPOSITORY_NAME = "__repository__"
JOB_NAME = "__ASSET_JOB"

LAUNCH_RUN_MUTATION = """
mutation LaunchRun($executionParams: ExecutionParams!) {
  launchRun(executionParams: $executionParams) {
    __typename
    ... on LaunchRunSuccess { run { runId status } }
    ... on PythonError { message }
    ... on RunConfigValidationInvalid { errors { message } }
    ... on InvalidSubsetError { message }
    ... on PipelineNotFoundError { message }
    ... on InvalidStepError { invalidStepKey }
  }
}
"""

RUN_STATUS_QUERY = """
query RunStatus($runId: ID!) {
  runOrError(runId: $runId) {
    __typename
    ... on Run { runId status }
    ... on RunNotFoundError { message }
  }
}
"""

def trigger_materialization(asset_selection: list[str], run_config: dict | None = None) -> str:
    """Launch a Dagster run materializing the given assets, return the run ID."""
    variables = {
        "executionParams": {
            "selector": {
                "repositoryLocationName": REPOSITORY_LOCATION_NAME,
                "repositoryName": REPOSITORY_NAME,
                "jobName": JOB_NAME,
                "assetSelection": [{"path": [name]} for name in asset_selection],
            },
            "runConfigData": run_config or {},
        }
    }
    response = httpx.post(
        DAGSTER_GRAPHQL_URL,
        json={"query": LAUNCH_RUN_MUTATION, "variables": variables},
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()["data"]["launchRun"]

    if result["__typename"] != "LaunchRunSuccess":
        raise RuntimeError(f"Failed to launch Dagster run: {result}")

    return result["run"]["runId"]


def wait_for_run(run_id: str, timeout: float = 120, poll_interval: float = 2.0) -> bool:
    """Poll a run until it reaches SUCCESS or FAILURE/CANCELED. Returns True on SUCCESS."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        response = httpx.post(
            DAGSTER_GRAPHQL_URL,
            json={"query": RUN_STATUS_QUERY, "variables": {"runId": run_id}},
            timeout=30,
        )
        response.raise_for_status()
        result = response.json()["data"]["runOrError"]

        if result["__typename"] != "Run":
            raise RuntimeError(f"Could not fetch status for run {run_id}: {result}")

        status = result["status"]
        if status == "SUCCESS":
            return True
        if status in ("FAILURE", "CANCELED"):
            return False

        time.sleep(poll_interval)

    raise TimeoutError(f"Run {run_id} did not complete within {timeout}s")