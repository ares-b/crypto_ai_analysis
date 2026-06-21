from dataclasses import dataclass
from datetime import date, datetime

from core.models import StoreRow


@dataclass(frozen=True)
class EtfFlow:
    flow_date: date
    net_flow_usd: float


class EtfFlowRow(StoreRow):
    date: date
    net_flow_usd: float
    source_updated_at: datetime


def build_rows(flows: list[EtfFlow], *, source_updated_at: datetime) -> list[EtfFlowRow]:
    return [
        EtfFlowRow(
            date=flow.flow_date,
            net_flow_usd=flow.net_flow_usd,
            source_updated_at=source_updated_at,
        )
        for flow in flows
    ]
