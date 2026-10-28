"""
Logger — structured run metrics saved as JSON.
A run is marked 'success' only after validation, reconciliation,
and publication have all succeeded.
"""
import json
import time
from pathlib import Path

from loguru import logger
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))
from config import LOGS_DIR


class RunLogger:
    def __init__(self, run_id: str, source_file: str, source_fingerprint: str):
        self.run_id = run_id
        self.source_file = source_file
        self.source_fingerprint = source_fingerprint
        self._start = time.perf_counter()
        self._start_ts = _utcnow()
        self.metrics: dict = {}

        LOGS_DIR.mkdir(parents=True, exist_ok=True)
        logger.add(
            LOGS_DIR / f"{run_id}.log",
            format="{time:YYYY-MM-DDTHH:mm:ss} | {level} | {message}",
            level="DEBUG",
        )
        logger.info(f"Run started | run_id={run_id} | source={source_file}")

    def record(self, **kwargs) -> None:
        self.metrics.update(kwargs)

    def finish(self, status: str, failure_reason: str = "") -> dict:
        end_ts = _utcnow()
        duration = round(time.perf_counter() - self._start, 3)
        input_rows = self.metrics.get("input_rows", 0)
        throughput = round(input_rows / duration, 1) if duration > 0 else 0.0

        log = {
            "run_id": self.run_id,
            "source_file": self.source_file,
            "source_fingerprint": self.source_fingerprint,
            "start_time": self._start_ts,
            "end_time": end_ts,
            "duration_seconds": duration,
            "throughput_rows_per_sec": throughput,
            "input_rows": input_rows,
            "newly_published_rows": self.metrics.get("newly_published_rows", 0),
            "total_published_rows": self.metrics.get("total_published_rows", 0),
            "rejected_rows": self.metrics.get("rejected_rows", 0),
            "duplicate_replay_rows": self.metrics.get("duplicate_replay_rows", 0),
            "conflicting_rows": self.metrics.get("conflicting_rows", 0),
            "status": status,
            "failure_reason": failure_reason,
        }

        path = LOGS_DIR / f"{self.run_id}.json"
        path.write_text(json.dumps(log, indent=2))

        level = "SUCCESS" if status == "success" else "ERROR"
        logger.log(level, f"Run finished | status={status} | duration={duration}s | {log}")
        return log


def _utcnow() -> str:
    import datetime
    return datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
