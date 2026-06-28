from dagster import Backoff, RetryPolicy


DEFAULT_RETRY_POLICY = RetryPolicy(max_retries=3, delay=10, backoff=Backoff.EXPONENTIAL)

def job_tags(
    *,
    memory_limit: str,
    cpu_request: str,
    memory_request: str,
    max_runtime: int,
) -> dict:
    return {
        "dagster/max_runtime": str(max_runtime),
        "dagster-k8s/config": {
            "container_config": {
                "resources": {
                    "requests": {"cpu": cpu_request, "memory": memory_request},
                    "limits": {"memory": memory_limit},
                }
            }
        },
    }


TIER_A = job_tags(memory_limit="1Gi", cpu_request="250m", memory_request="512Mi", max_runtime=900)
TIER_B = job_tags(memory_limit="768Mi", cpu_request="200m", memory_request="384Mi", max_runtime=900)
TIER_C = job_tags(memory_limit="640Mi", cpu_request="100m", memory_request="320Mi", max_runtime=300)
