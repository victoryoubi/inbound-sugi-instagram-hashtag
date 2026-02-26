from __future__ import annotations

import json
import os
import random
import signal
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Iterable, TypeVar

from google.cloud import bigquery

T = TypeVar("T")

PROJECT_ID = "inbound-core"
WORKFLOW_NAME = "naver-article-product-matching-pipeline"
SNAPSHOT_TABLE = "inbound-core.search.product_demand_serp_snapshots"
JOB_RUNS_TABLE = "inbound-core.workflows.pipeline_job_runs"

# ====== Default timeouts (tune as needed) ======
DEFAULT_BIGQUERY_JOB_TIMEOUT_MS = 55_000
DEFAULT_BIGQUERY_WAIT_TIMEOUT_SECONDS = 75  # job_timeout より余裕を持たせる

DEFAULT_GEMINI_HTTP_TIMEOUT_MS = 180_000  # 180秒（gemini-2.5-pro 対応）
DEFAULT_EMBEDDING_HTTP_TIMEOUT_MS = 30_000

# 重いクエリ用（VECTOR_SEARCH, MERGE など）
HEAVY_QUERY_JOB_TIMEOUT_MS = 120_000
HEAVY_QUERY_WAIT_TIMEOUT_SECONDS = 150  # job_timeout より余裕を持たせる


def env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"Missing env: {name}")
    return value


def is_retryable_error(exception: Exception) -> bool:
    """再試行可能な一時エラーかどうか（429, 503, 504, タイムアウト系）"""
    message = str(exception).lower()
    error_patterns = [
        "429",
        "503",
        "504",
        "deadline",
        "timeout",
        "timed out",
        "socket",
        "connection reset",
        "connection aborted",
    ]
    return any(pattern in message for pattern in error_patterns)


def now_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def retry(fn: Callable[[], T], max_retries: int = 1) -> T:
    for attempt in range(max_retries):
        try:
            return fn()
        except Exception as exception:
            message = str(exception)
            if (("429" in message) or ("503" in message)) and attempt < max_retries - 1:
                time.sleep((2**attempt) + random.uniform(0, 1))
                continue
            raise
    raise RuntimeError("retry: unreachable")


class OperationTimeoutError(TimeoutError):
    """タイムアウトによる操作中断を示す例外"""

    pass


# ネスト検出用フラグ（単一スレッド前提）
_deadline_active = False


@contextmanager
def deadline_seconds(timeout_seconds: int, *, operation_name: str) -> Iterable[None]:
    """
    最後の保険: Python レベルで "必ず戻る" deadline を付与する。

    ⚠️ 制約（必ず守ること）:
    - メインスレッド限定: ThreadPoolExecutor 内などでは効きません
    - ネスト時は内側が no-op になる（外側のタイムアウトが優先）

    Cloud Run (Linux) 前提で signal を使う。
    外部ライブラリが無期限ブロックしても、OSのシグナルで割り込めるケースが多い。
    """
    global _deadline_active

    if timeout_seconds <= 0:
        yield
        return

    if _deadline_active:
        # すでに外側で deadline が有効なら、内側は no-op にする（安全優先）
        yield
        return

    _deadline_active = True
    previous_handler = signal.getsignal(signal.SIGALRM)

    def _handler(_signum: int, _frame: Any) -> None:
        raise OperationTimeoutError(f"{operation_name} timed out after {timeout_seconds}s")

    signal.signal(signal.SIGALRM, _handler)
    signal.setitimer(signal.ITIMER_REAL, timeout_seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        _deadline_active = False


def create_genai_client(
    *,
    api_key: str | None = None,
    http_timeout_ms: int = DEFAULT_GEMINI_HTTP_TIMEOUT_MS,
) -> Any:
    """
    Gemini クライアント生成を共通化。
    HttpOptions.timeout はミリ秒指定。

    - api_key: Gemini Developer API 用（API キー認証）
    - api_key なし: ADC (Application Default Credentials) を使用
    """
    from google import genai
    from google.genai import types

    if api_key:
        return genai.Client(
            api_key=api_key,
            http_options=types.HttpOptions(timeout=http_timeout_ms),
        )
    else:
        return genai.Client(
            http_options=types.HttpOptions(timeout=http_timeout_ms),
        )


def bigquery_query_rows(
    bigquery_client: bigquery.Client,
    query: str,
    *,
    job_config: bigquery.QueryJobConfig,
    operation_name: str = "bigquery_query",
    heavy: bool = False,
) -> list[Any]:
    """
    BigQuery クエリの「無期限待機」を防ぐ共通関数。

    - job_timeout_ms: サーバ側でクエリを止める試み
    - timeout: クライアント側の HTTP/待機の上限
    - deadline_seconds: 最後の保険（signal ベース）

    heavy=True の場合は VECTOR_SEARCH / MERGE など重いクエリ用のタイムアウトを使用。
    """
    if heavy:
        job_timeout_ms = HEAVY_QUERY_JOB_TIMEOUT_MS
        wait_timeout_seconds = HEAVY_QUERY_WAIT_TIMEOUT_SECONDS
    else:
        job_timeout_ms = DEFAULT_BIGQUERY_JOB_TIMEOUT_MS
        wait_timeout_seconds = DEFAULT_BIGQUERY_WAIT_TIMEOUT_SECONDS

    job_config.job_timeout_ms = job_timeout_ms

    query_job = bigquery_client.query(
        query,
        job_config=job_config,
        timeout=wait_timeout_seconds,
    )

    try:
        with deadline_seconds(wait_timeout_seconds, operation_name=operation_name):
            iterator = query_job.result(timeout=wait_timeout_seconds)
            return list(iterator)
    except Exception as exception:
        # 原因追跡のため job_id をログに残す
        print(
            json.dumps(
                {
                    "level": "error",
                    "operation": operation_name,
                    "bq_job_id": query_job.job_id,
                    "error_type": type(exception).__name__,
                    "error": str(exception),
                },
                ensure_ascii=False,
            )
        )
        try:
            query_job.cancel()
        except Exception:
            pass
        raise


def load_json_rows(
    bq: bigquery.Client,
    table_id: str,
    rows: list[dict[str, Any]],
) -> int:
    """
    BigQuery テーブルに JSON 行を追加する。
    タイムアウト設定済み。
    """
    if not rows:
        return 0

    wait_timeout_seconds = DEFAULT_BIGQUERY_WAIT_TIMEOUT_SECONDS

    job = bq.load_table_from_json(
        rows,
        table_id,
        job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
            ignore_unknown_values=True,
        ),
        timeout=wait_timeout_seconds,
    )

    try:
        with deadline_seconds(wait_timeout_seconds, operation_name="load_json_rows"):
            job.result(timeout=wait_timeout_seconds)
    except Exception as exception:
        print(
            json.dumps(
                {
                    "level": "error",
                    "operation": "load_json_rows",
                    "table_id": table_id,
                    "bq_job_id": job.job_id,
                    "error_type": type(exception).__name__,
                    "error": str(exception),
                },
                ensure_ascii=False,
            )
        )
        raise

    output_rows = getattr(job, "output_rows", None)
    return int(output_rows) if isinstance(output_rows, int) else len(rows)
