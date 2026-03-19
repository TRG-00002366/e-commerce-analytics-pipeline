from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from util.logging import get_logger

logger = get_logger(__name__)

# Deduplicates streaming events based on event_id using watermark.
def deduplicate_events(
    df: DataFrame,
    event_id_col: str = "event_id",
    timestamp_col: str = "timestamp",
    watermark_delay: str = "10 minutes"
):

    logger.info(
        f"Applying deduplication on '{event_id_col}' with watermark '{watermark_delay}'"
    )

    df_clean = df.filter(col(timestamp_col).isNotNull())

    # Apply watermark
    df_with_watermark = df_clean.withWatermark(timestamp_col, watermark_delay)

    # Drop duplicates based on event_id
    df_dedup = df_with_watermark.dropDuplicates([event_id_col])

    return df_dedup