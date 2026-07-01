from collections.abc import Sequence

from dagster import AssetExecutionContext, AssetKey, MaterializeResult, RetryPolicy, asset

from core.quality import QualitySubject, RunResult
from orchestration._runtime import DEFAULT_RETRY_POLICY
from orchestration.checks import build_check_specs, to_check_results

_KEY_PREFIX = ["crypto-ai-analysis"]


def asset_factory(
    *,
    name: str,
    group_name: str,
    partitions_def,
    tags: dict[str, str],
    subjects: Sequence[QualitySubject] = (),
    compute_kind: str = "python",
    retry_policy: RetryPolicy = DEFAULT_RETRY_POLICY,
):
    key = AssetKey([*_KEY_PREFIX, name])
    return asset(
        name=name,
        key_prefix=_KEY_PREFIX,
        group_name=group_name,
        compute_kind=compute_kind,
        retry_policy=retry_policy,
        tags=tags,
        partitions_def=partitions_def,
        check_specs=build_check_specs(key, subjects),
    )


def raw_asset(
    *,
    name: str,
    source: str,
    partitions_def,
    subjects: Sequence[QualitySubject],
):
    return asset_factory(
        name=name,
        group_name="raw",
        partitions_def=partitions_def,
        tags={"source": source},
        subjects=subjects,
    )


def to_materialize_result(context: AssetExecutionContext, result: RunResult) -> MaterializeResult:
    return MaterializeResult(
        asset_key=context.asset_key,
        metadata=dict(result),
        check_results=list(to_check_results(context.asset_key, result.reports)),
    )
