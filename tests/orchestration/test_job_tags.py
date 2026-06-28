"""Guard rails for run-pod sizing tags on raw jobs.

Orchestration wiring is otherwise not unit-tested (needs real Dagster/k8s),
but the resource and timeout tags are pure data: a missing tag silently falls
back to the cluster default and breaks per-job sizing, so assert presence and
shape here. Walks the layer like the app does, so new modules are covered
automatically.
"""

import importlib
import json
import pkgutil

import pytest

import orchestration.raw as raw_layer

_K8S_TAG = "dagster-k8s/config"
_RUNTIME_TAG = "dagster/max_runtime"


def _raw_modules():
    for info in pkgutil.walk_packages(raw_layer.__path__, prefix=raw_layer.__name__ + "."):
        yield importlib.import_module(info.name)


def _all_jobs():
    for mod in _raw_modules():
        yield from getattr(mod, "JOBS", [])


def _all_assets():
    for mod in _raw_modules():
        yield from getattr(mod, "ASSETS", [])


_JOBS = list(_all_jobs())
_ASSETS = list(_all_assets())


def test_raw_jobs_discovered():
    assert _JOBS, "no raw jobs discovered"


@pytest.mark.parametrize("job", _JOBS, ids=lambda j: j.name)
def test_job_has_resource_and_timeout_tags(job):
    tags = job.tags

    runtime = tags.get(_RUNTIME_TAG)
    assert runtime is not None, f"{job.name} missing {_RUNTIME_TAG}"
    assert int(runtime) > 0

    raw_cfg = tags.get(_K8S_TAG)
    assert raw_cfg is not None, f"{job.name} missing {_K8S_TAG}"
    resources = json.loads(raw_cfg)["container_config"]["resources"]

    requests, limits = resources["requests"], resources["limits"]
    assert requests["cpu"] and requests["memory"]
    # Memory limit is the OOM guard; CPU limit is intentionally omitted.
    assert limits["memory"]
    assert "cpu" not in limits, f"{job.name} sets a CPU limit (CFS throttling risk)"


@pytest.mark.parametrize("asset_def", _ASSETS, ids=lambda a: a.key.to_python_identifier())
def test_asset_has_retry_policy(asset_def):
    policy = asset_def.op.retry_policy
    assert policy is not None, f"{asset_def.key} missing retry policy"
    assert policy.max_retries >= 1
