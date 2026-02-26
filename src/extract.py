from __future__ import annotations

import json
from typing import Any

from google import genai
from google.genai import types
from google.cloud import bigquery

from common.common import (
    JOB_RUNS_TABLE,
    PROJECT_ID,
    WORKFLOW_NAME,
    DEFAULT_GEMINI_HTTP_TIMEOUT_MS,
    env,
    is_retryable_error,
    load_json_rows,
    now_utc,
    retry,
    create_genai_client,
    bigquery_query_rows,
)

STEP_NAME = "post-product-name-extractor"  

MODEL_NAME = "gemini-2.5-pro"
TEXT_LIMIT_CHARS = 12_000

BATCH_LIMIT_ROWS = 50
MAX_TOTAL_ROWS = 1000
ERROR_SAMPLE_LIMIT = 10

NO_PRODUCT_SENTINEL = "__NO_PRODUCT__"


def parse_string_list(value: str) -> list[str]:
    try:
        data = json.loads(value or "[]")
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    results: list[str] = []
    for item in data:
        if isinstance(item, str):
            stripped = item.strip()
            if stripped:
                results.append(stripped)
    return results


def extract_product_names_once(genai_client: genai.Client, text: str) -> list[str]:
    """
    Instagram caption 等の短文から、商品名/ブランド名を抽出する。
    """
    prompt = f"""
あなたは「小売店で販売される商品名/ブランド名」を抽出するシステムです。
次の入力（投稿文）から、商品名またはブランド名に該当する固有名詞を抽出してください。

前処理:
- 明らかな省略・表記ゆれ・誤字は、本文に根拠がある範囲で正しい表現に整える
- 韓国語は韓国語のまま出力する
- ハッシュタグ（#〇〇）は、文脈上それが商品名/ブランド名だと判断できる場合のみ採用する（ノイズが多いので慎重に）

抽出対象（入れるもの）:
- 小売店で購入できる「商品名」または「ブランド名」
- 投稿文に登場し、文脈上それが商品/ブランドである根拠があるもの
- 一般名詞単体（例: クリーム、化粧水、タクシー など）は除外（ただしブランド名/商品名として明確な場合のみ可）

ノイズ除外（基本入れないもの）:
- 地名/観光地/建物/寺社/施設（例: 金閣寺）
- アプリ名/サービス名/プラットフォーム
- 会社名/組織名/人物名/イベント名/作品名
- 店舗名（ただしブランドとして一般に流通している場合は可）
- 投稿の文脈で「買う/使う/食べる」対象になっていない固有名詞
- PR表記（PR, 提供, 広告 など）はノイズ

例外（ノイズに見えても入れてよい条件）:
- 地名や固有名詞（例: 金閣寺）が、投稿文の文脈で「商品名（例: お菓子・お土産・銘柄・商品シリーズ名）」として明確に扱われている場合は抽出してよい
  - 例: 商品として説明されている／価格・購入・味・容量など商品属性が語られている

出力ルール:
- 出力は JSON の配列のみ（例: ["A", "B"]）。説明文は禁止。
- 文字列は重複しないようにし、出現順に並べる
- 迷う場合は「除外」する（誤検出より欠落を優先）

投稿文:
{text}
""".strip()

    response = genai_client.models.generate_content(
        model=MODEL_NAME,
        contents=prompt,
        config=types.GenerateContentConfig(
            response_mime_type="application/json",
            response_json_schema={"type": "array", "items": {"type": "string"}},
            temperature=0.0,
        ),
    )
    return parse_string_list(response.text or "[]")


def is_rate_limited(exception: Exception) -> bool:
    return "429" in str(exception)


def fetch_pending_rows(
    bq: bigquery.Client,
    *,
    input_table: str,
    output_table: str,
    pk_col: str,
    text_col: str,
    limit: int,
) -> list[Any]:
    # 列名はパラメータ化できないので f-string で埋め込む（envが固定列名前提）
    query = f"""
    SELECT
      a.{pk_col} AS pk,
      a.{text_col} AS text
    FROM `{input_table}` a
    WHERE a.{text_col} IS NOT NULL
      AND LENGTH(a.{text_col}) >= 20
      AND NOT EXISTS (
        SELECT 1 FROM `{output_table}` o WHERE o.{pk_col} = a.{pk_col}
      )
    ORDER BY a.fetched_at DESC
    LIMIT @limit
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)]
    )
    return bigquery_query_rows(bq, query, job_config=job_config, operation_name="fetch_pending_rows")


def run_extract() -> None:
    input_table = env("INPUT_TABLE")
    output_table = env("OUTPUT_TABLE")
    pk_col = env("PRIMARY_KEY_COLUMN")   # 例: id
    text_col = env("TEXT_COLUMN")        # 例: caption

    bq = bigquery.Client(project=PROJECT_ID)
    genai_client = create_genai_client(
        api_key=env("GOOGLE_GENERATIVE_AI_API_KEY"),
        http_timeout_ms=DEFAULT_GEMINI_HTTP_TIMEOUT_MS,
    )

    run_at = now_utc()
    attempted = 0
    success = 0
    errors = 0
    rate_limited = False
    error_samples: list[dict[str, Any]] = []

    while attempted < MAX_TOTAL_ROWS and not rate_limited:
        pending_rows = fetch_pending_rows(
            bq,
            input_table=input_table,
            output_table=output_table,
            pk_col=pk_col,
            text_col=text_col,
            limit=min(BATCH_LIMIT_ROWS, MAX_TOTAL_ROWS - attempted),
        )
        if not pending_rows:
            break

        output_rows: list[dict[str, Any]] = []

        for row in pending_rows:
            attempted += 1
            pk = row.pk
            text = (row.text or "")[:TEXT_LIMIT_CHARS]

            try:
                try:
                    names = extract_product_names_once(genai_client, text)
                except Exception as inner_exception:
                    if is_rate_limited(inner_exception):
                        rate_limited = True
                        errors += 1
                        if len(error_samples) < ERROR_SAMPLE_LIMIT:
                            error_samples.append({"pk": pk, "message": f"rate_limited: {inner_exception}"})
                        break
                    if is_retryable_error(inner_exception):
                        errors += 1
                        if len(error_samples) < ERROR_SAMPLE_LIMIT:
                            error_samples.append({"pk": pk, "message": f"retryable_error: {inner_exception}"})
                        continue
                    raise

                if not names:
                    output_rows.append({pk_col: pk, "product_name": NO_PRODUCT_SENTINEL, "extracted_at": run_at})
                    success += 1
                    continue

                for name in names:
                    output_rows.append({pk_col: pk, "product_name": name, "extracted_at": run_at})
                success += 1
            except Exception as exception:
                errors += 1
                if len(error_samples) < ERROR_SAMPLE_LIMIT:
                    error_samples.append({"pk": pk, "message": str(exception)})

        try:
            retry(lambda: load_json_rows(bq, output_table, output_rows), max_retries=3)
        except Exception as exception:
            errors += len(output_rows)
            if len(error_samples) < ERROR_SAMPLE_LIMIT:
                error_samples.append({"message": f"output_load_failed: {exception}"})

    status = "rate_limited" if rate_limited else ("success" if errors == 0 else "error")

    # 実行ログ（BQ）
    retry(
        lambda: load_json_rows(
            bq,
            JOB_RUNS_TABLE,
            [
                {
                    "run_at": run_at,
                    "workflow_name": WORKFLOW_NAME,  # common の値（テストなので一旦OK）
                    "step_name": STEP_NAME,
                    "job_name": STEP_NAME,
                    "status": status,
                    "processed_count": attempted,
                    "success_count": success,
                    "error_count": errors,
                    "error_json": {"samples": error_samples},
                }
            ],
        ),
        max_retries=3,
    )

    print(
        json.dumps(
            {
                "status": status,
                "attempted": attempted,
                "success": success,
                "errors": errors,
                "rate_limited": rate_limited,
                "input_table": input_table,
                "output_table": output_table,
                "pk_col": pk_col,
                "text_col": text_col,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    run_extract()
