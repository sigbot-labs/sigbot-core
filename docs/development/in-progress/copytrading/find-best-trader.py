#!/usr/bin/env python3
"""
Temporary tool: fetch Binance Copy Trading lead portfolios from public web APIs
and rank candidates by conservative preference rules.

Notes:
- Binance's official Copy Trading SAPI endpoints are mostly account/auth related.
  Public lead-portfolio rankings are exposed by Binance's web BAPI and may change.
- This script uses only Python standard library modules.
- This is for research/screening only; always verify candidates manually on Binance.

Examples:
  python docs/development/in-progress/copytrading/find-best-trader.py
  python docs/development/in-progress/copytrading/find-best-trader.py --pages 5 --format csv
  python docs/development/in-progress/copytrading/find-best-trader.py --proxy http://127.0.0.1:8800
  python docs/development/in-progress/copytrading/find-best-trader.py --endpoint URL
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Iterable


DEFAULT_ENDPOINTS = [
    # Undocumented but token-free Binance Copy Trading JSON endpoint.
    "https://www.binance.com/bapi/futures/v1/friendly/future/copy-trade/home-page/query-list",
]

DEFAULT_PAGE_SIZE = 30


@dataclass
class Trader:
    portfolio_id: str
    nickname: str
    sharpe_ratio: float | None
    max_drawdown: float | None
    win_rate: float | None
    roi: float | None
    aum: float | None
    followers: int | None
    raw: dict[str, Any]

    @property
    def win_rate_distance(self) -> float:
        """Distance from the preferred 50%-60% band; 0 means inside the band."""
        if self.win_rate is None:
            return math.inf
        if 0.50 <= self.win_rate <= 0.60:
            return 0.0
        return min(abs(self.win_rate - 0.50), abs(self.win_rate - 0.60))

    def passes_hard_filters(self, args: argparse.Namespace) -> bool:
        passes_core_filters = (
            self.sharpe_ratio is not None
            and self.sharpe_ratio > args.min_sharpe
            and self.max_drawdown is not None
            and self.max_drawdown < args.max_drawdown
            and self.roi is not None
            and self.roi > args.min_roi
        )
        if not passes_core_filters:
            return False
        if not args.strict_win_rate:
            return True
        return (
            self.win_rate is not None
            and args.min_win_rate <= self.win_rate <= args.max_win_rate
        )

    def rank_key(self) -> tuple[float, float, float, float, float, float]:
        # User preference order: Sharpe first, drawdown second, win-rate band third, ROI fourth.
        return (
            self.sharpe_ratio if self.sharpe_ratio is not None else -math.inf,
            -(self.max_drawdown if self.max_drawdown is not None else math.inf),
            -self.win_rate_distance,
            self.roi if self.roi is not None else -math.inf,
            self.aum if self.aum is not None else -math.inf,
            float(self.followers if self.followers is not None else -1),
        )

    def to_row(self) -> dict[str, Any]:
        return {
            "portfolio_id": self.portfolio_id,
            "nickname": self.nickname,
            "sharpe_ratio": self.sharpe_ratio,
            "max_drawdown_pct": percent_for_output(self.max_drawdown),
            "win_rate_pct": percent_for_output(self.win_rate),
            "roi_pct": percent_for_output(self.roi),
            "aum": self.aum,
            "followers": self.followers,
        }


def percent_for_output(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value * 100, 4)


def first_value(data: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        if key in data and data[key] not in ("", None):
            return data[key]
    return None


def to_number(value: Any) -> float | None:
    if value in ("", None):
        return None
    if isinstance(value, (int, float)):
        result = float(value)
    else:
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        is_percent = text.endswith("%")
        if is_percent:
            text = text[:-1].strip()
        try:
            result = float(text)
        except ValueError:
            return None
    return result


def to_percent_ratio(value: Any) -> float | None:
    result = to_number(value)
    if result is None:
        return None
    # Current Binance Copy Trading BAPI returns numeric percentages, e.g. 60.0 for 60%.
    return result / 100


def to_int(value: Any) -> int | None:
    if value in ("", None):
        return None
    try:
        return int(float(str(value).replace(",", "")))
    except ValueError:
        return None


def parse_trader(item: dict[str, Any]) -> Trader:
    portfolio_id = str(
        first_value(item, ["portfolioId", "portfolio_id", "id", "leadPortfolioId"]) or ""
    )
    nickname = str(first_value(item, ["nickname", "nickName", "leadTraderName", "name"]) or "")

    sharpe_ratio = to_number(
        first_value(item, ["sharpRatio", "sharpeRatio", "sharpe", "sharp"])
    )
    max_drawdown = to_percent_ratio(
        first_value(item, ["maxDrawdown", "mdd", "drawdown", "maximumDrawdown"])
    )
    win_rate = to_percent_ratio(first_value(item, ["winRate", "winRatio", "positionWinRatio"]))
    roi = to_percent_ratio(first_value(item, ["roi", "profitRate", "yieldRate", "returnRate"]))
    aum = to_number(first_value(item, ["aum", "copyAmount", "totalCopyAmount", "marginBalance"]))
    followers = to_int(
        first_value(
            item,
            [
                "currentCopyCount",
                "copyCount",
                "copierCount",
                "followers",
                "followerCount",
            ],
        )
    )

    return Trader(
        portfolio_id=portfolio_id,
        nickname=nickname,
        sharpe_ratio=sharpe_ratio,
        max_drawdown=max_drawdown,
        win_rate=win_rate,
        roi=roi,
        aum=aum,
        followers=followers,
        raw=item,
    )


def extract_items(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data", payload)
    if isinstance(data, list):
        return [item for item in data if isinstance(item, dict)]

    if not isinstance(data, dict):
        return []

    candidates = [
        data.get("list"),
        data.get("rows"),
        data.get("items"),
        data.get("portfolioList"),
        data.get("leaderList"),
        data.get("rankList"),
    ]
    for candidate in candidates:
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]

    return []


def post_json(
    url: str,
    payload: dict[str, Any],
    timeout: float,
    proxy: str | None,
) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "Content-Type": "application/json",
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36"
            ),
            "Origin": "https://www.binance.com",
            "Referer": "https://www.binance.com/en/copy-trading",
            "clienttype": "web",
        },
        method="POST",
    )
    if proxy:
        opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({"http": proxy, "https": proxy})
        )
        response_ctx = opener.open(request, timeout=timeout)
    else:
        response_ctx = urllib.request.urlopen(request, timeout=timeout)

    with response_ctx as response:
        return json.loads(response.read().decode("utf-8"))


def request_payload(page: int, page_size: int) -> dict[str, Any]:
    # Fields are intentionally redundant because Binance has changed names before.
    return {
        "pageNumber": page,
        "pageSize": page_size,
        "page": page,
        "rows": page_size,
        "sort": "SHARP_RATIO",
        "sortType": "DESC",
        "timeRange": "30D",
        "dataType": "SHARP_RATIO",
        "favoriteOnly": False,
        "hideFull": False,
    }


def fetch_page(
    endpoint: str,
    page: int,
    page_size: int,
    timeout: float,
    proxy: str | None,
    verbose: bool,
) -> list[dict[str, Any]]:
    payload = request_payload(page, page_size)
    if verbose:
        print(f"fetch page={page} endpoint={endpoint}", file=sys.stderr)
    response = post_json(endpoint, payload, timeout, proxy)
    code = response.get("code")
    success = response.get("success")
    if code not in (None, "000000", "0") and success is False:
        raise RuntimeError(f"Binance returned code={code}: {response}")
    return extract_items(response)


def fetch_traders(args: argparse.Namespace) -> list[Trader]:
    endpoints = [args.endpoint] if args.endpoint else DEFAULT_ENDPOINTS
    last_error: Exception | None = None

    for endpoint in endpoints:
        traders: list[Trader] = []
        try:
            for page in range(1, args.pages + 1):
                items = fetch_page(
                    endpoint,
                    page,
                    args.page_size,
                    args.timeout,
                    args.proxy,
                    args.verbose,
                )
                if not items:
                    break
                traders.extend(parse_trader(item) for item in items)
                if len(items) < args.page_size:
                    break
                time.sleep(args.sleep)
            if traders:
                return traders
        except (urllib.error.URLError, TimeoutError, RuntimeError, json.JSONDecodeError) as exc:
            last_error = exc
            if args.verbose:
                print(f"endpoint failed: {endpoint}: {exc}", file=sys.stderr)

    if last_error is not None:
        raise SystemExit(f"failed to fetch Binance public copy-trading data: {last_error}")
    raise SystemExit("failed to fetch Binance public copy-trading data: no rows returned")


def emit_json(rows: list[dict[str, Any]]) -> None:
    print(json.dumps(rows, ensure_ascii=False, indent=2))


def emit_csv(rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Find Binance Copy Trading lead portfolios matching simple quality filters."
    )
    parser.add_argument("--endpoint", help="override Binance public BAPI endpoint")
    parser.add_argument(
        "--proxy",
        default=os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy"),
        help="HTTP/HTTPS proxy, e.g. http://127.0.0.1:8800",
    )
    parser.add_argument("--pages", type=int, default=3, help="number of pages to fetch")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE)
    parser.add_argument("--timeout", type=float, default=15.0)
    parser.add_argument("--sleep", type=float, default=0.2, help="sleep between pages")
    parser.add_argument("--limit", type=int, default=30, help="max rows to output")
    parser.add_argument("--format", choices=["json", "csv"], default="json")
    parser.add_argument("--verbose", action="store_true")

    parser.add_argument("--min-sharpe", type=float, default=10.0)
    parser.add_argument("--max-drawdown", type=float, default=0.10)
    parser.add_argument("--min-win-rate", type=float, default=0.50)
    parser.add_argument("--max-win-rate", type=float, default=0.60)
    parser.add_argument(
        "--strict-win-rate",
        action="store_true",
        help="make win-rate range a hard filter instead of a ranking preference",
    )
    parser.add_argument("--min-roi", type=float, default=0.20)
    return parser


def main() -> int:
    args = build_parser().parse_args()
    traders = fetch_traders(args)
    seen: set[str] = set()
    unique_traders: list[Trader] = []
    for trader in traders:
        key = trader.portfolio_id or json.dumps(trader.raw, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        unique_traders.append(trader)

    filtered = [trader for trader in unique_traders if trader.passes_hard_filters(args)]
    filtered.sort(key=lambda trader: trader.rank_key(), reverse=True)

    rows = [trader.to_row() for trader in filtered[: args.limit]]
    if args.format == "csv":
        emit_csv(rows)
    else:
        emit_json(rows)

    if args.verbose:
        print(
            f"fetched={len(unique_traders)} matched={len(filtered)} output={len(rows)}",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
