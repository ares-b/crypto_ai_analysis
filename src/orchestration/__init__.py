import importlib
import pkgutil
from collections import namedtuple
from types import ModuleType

import orchestration.raw as _raw
from dagster import Definitions

from orchestration.resources import ALL_RESOURCES

_Definitions = namedtuple("_Definitions", ["assets", "jobs", "schedules"])

_LAYERS: list[ModuleType] = [_raw]


def _discover(*layers: ModuleType) -> _Definitions:
    assets: list = []
    jobs: list = []
    schedules: list = []

    for layer in layers:
        for mod_info in pkgutil.walk_packages(layer.__path__, prefix=layer.__name__ + "."):
            mod = importlib.import_module(mod_info.name)
            assets.extend(getattr(mod, "ASSETS", []))
            jobs.extend(getattr(mod, "JOBS", []))
            schedules.extend(getattr(mod, "SCHEDULES", []))

    return _Definitions(assets, jobs, schedules)


_defs = _discover(*_LAYERS)

defs = Definitions(
    assets=_defs.assets,
    jobs=_defs.jobs,
    schedules=_defs.schedules,
    resources=ALL_RESOURCES,
)
