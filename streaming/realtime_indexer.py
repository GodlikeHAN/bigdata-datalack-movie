from __future__ import annotations

import pandas as pd

from src.config.settings import REALTIME_TRENDING_INDEX
from src.indexing.bulk_indexer import bulk_index_dataframe


def index_realtime_events(events: list[dict]) -> int:
    if not events:
        return 0
    dataframe = pd.DataFrame(events)
    return bulk_index_dataframe(REALTIME_TRENDING_INDEX, dataframe)
