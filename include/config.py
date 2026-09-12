"""Registry of the BCB SGS series this pipeline ingests.

Adding a new series is a one-line change here: the DAG maps dynamically over
this registry, so no DAG code changes when the list grows.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

Frequency = Literal["daily", "monthly"]


@dataclass(frozen=True)
class Series:
    code: int           # BCB SGS series code
    name: str           # snake_case name, used as the partition folder
    frequency: Frequency
    unit: str
    # Sanity bounds used by the quality check. Deliberately wide: this guards
    # against parsing/scale mistakes, not against real market moves.
    min_value: float
    max_value: float

    def as_dict(self) -> dict:
        """Airflow maps over plain dicts — dataclasses are not XCom friendly."""
        return {
            "code": self.code,
            "name": self.name,
            "frequency": self.frequency,
            "unit": self.unit,
            "min_value": self.min_value,
            "max_value": self.max_value,
        }


SERIES: tuple[Series, ...] = (
    Series(432, "selic_meta", "daily", "percent_per_year", 0.0, 100.0),
    Series(1, "usd_brl_ptax", "daily", "brl_per_usd", 0.5, 50.0),
    Series(433, "ipca", "monthly", "percent_per_month", -10.0, 10.0),
    Series(189, "igpm", "monthly", "percent_per_month", -10.0, 10.0),
)

SERIES_BY_NAME = {s.name: s for s in SERIES}


def all_series_as_dicts() -> list[dict]:
    return [s.as_dict() for s in SERIES]
