import importlib
import pkgutil

import pipelines

from core.iceberg import IcebergRecord, TableSpec


def _collect_specs() -> tuple[TableSpec, ...]:
    # Import every pipeline module so IcebergRecord subclasses register their TABLE_SPEC.
    for _mod in pkgutil.walk_packages(pipelines.__path__, prefix="pipelines."):
        importlib.import_module(_mod.name)

    specs: list[TableSpec] = []
    queue = list(IcebergRecord.__subclasses__())
    while queue:
        cls = queue.pop()
        queue.extend(cls.__subclasses__())
        if "TABLE_SPEC" in cls.__dict__:
            specs.append(cls.TABLE_SPEC)
    return tuple(specs)


ALL_SPECS: tuple[TableSpec, ...] = _collect_specs()
