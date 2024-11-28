import relbench

import duckdb
import pandas as pd

from relbench.base import Database, EntityTask, Table, TaskType
from relbench.datasets import get_dataset
from relbench.metrics import accuracy, average_precision, f1, roc_auc
from relbench.tasks import get_task, get_task_names, register_task

from app.rel_deep.create_dataset import TennisATPDataset


class TennisTop3Task(EntityTask):
    r"""Predict if each driver will qualify in the top-3 for a race within the next 1
    month."""

    task_type = TaskType.BINARY_CLASSIFICATION
    entity_col = "player"
    entity_table = "players"
    time_col = "date"
    target_col = "had_final_or_semifinal"
    timedelta = pd.Timedelta(days=30)
    metrics = [average_precision, accuracy, f1, roc_auc]
    num_eval_timestamps = 40

    def make_table(self, db: Database, timestamps: "pd.Series[pd.Timestamp]") -> Table:
        timestamp_df = pd.DataFrame({"timestamp": timestamps})

        match_stats = db.table_dict["match_stats"].df
        matches = db.table_dict["matches"].df

        df = duckdb.sql(
            f"""
                SELECT
                    t.timestamp as date,
                    ms.player as player,
                    CASE
                        WHEN MIN(m.level) <= 2 THEN 1
                        ELSE 0
                    END AS had_final_or_semifinal
                FROM
                    timestamp_df t
                LEFT JOIN
                    matches m
                ON
                    m.date <= t.timestamp + INTERVAL '{self.timedelta}'
                    and m.date > t.timestamp
                LEFT JOIN
                    match_stats ms
                ON
                    m.match = ms.match
                GROUP BY t.timestamp, ms.player
            ;
            """
        ).df()

        df["had_final_or_semifinal"] = df["had_final_or_semifinal"].astype("int64")

        return Table(
            df=df,
            fkey_col_to_pkey_table={self.entity_col: self.entity_table},
            pkey_col=None,
            time_col=self.time_col,
        )

tennis_dataset = TennisATPDataset(cache_dir="./cache/atp")
tennis_top3_task = TennisTop3Task(tennis_dataset, cache_dir="./cache/driver_dnf")
print(tennis_top3_task.get_table("train"))