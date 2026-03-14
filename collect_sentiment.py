#!/usr/bin/env python3
"""
HerdVibe 종합 센티먼트 수집기
무료 API만 사용: CNN F&G, ApeWisdom, LunarCrush Public, Finnhub
GitHub Actions에서 매일 실행
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

# requests는 pip install 필요
import requests

DATA_DIR = os.environ.get("DATA_DIR", "data")
os.makedirs(DATA_DIR, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}


# ──────────────────────────────────────────────
# 1. CNN Fear & Greed Index (주식)
# ──────────────────────────────────────────────
def fetch_cnn_fear_greed():
    """CNN Fear & Greed 종합 + 7개 하위 지표 수집"""
    print("[1/4] CNN Fear & Greed Index 수집 중...")
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        raw = resp.json()

        fg = raw.get("fear_and_greed", {})
        result = {
            "score": round(fg.get("score", 0), 1),
            "rating": fg.get("rating", ""),
            "previous_close": round(fg.get("previous_close", 0), 1),
            "week_ago": round(fg.get("previous_1_week", 0), 1),
            "month_ago": round(fg.get("previous_1_month", 0), 1),
            "year_ago": round(fg.get("previous_1_year", 0), 1),
            "timestamp": fg.get("timestamp", ""),
            "indicators": {},
        }

        # 7개 하위 지표
        indicator_keys = [
            "stock_price_momentum",
            "stock_price_strength",
            "stock_price_breadth",
            "put_call_options",
            "market_volatility_vix",
            "safe_haven_demand",
            "junk_bond_demand",
        ]
        indicator_names_kr = {
            "stock_price_momentum": "주가 모멘텀",
            "stock_price_strength": "주가 강도",
            "stock_price_breadth": "주가 폭",
            "put_call_options": "풋/콜 옵션",
            "market_volatility_vix": "시장 변동성(VIX)",
            "safe_haven_demand": "안전자산 수요",
            "junk_bond_demand": "정크본드 수요",
        }

        for key in indicator_keys:
            ind = raw.get("fear_and_greed_historical", {}).get(key, [{}])
            # CNN returns list with latest entry
            if isinstance(ind, dict):
                score_val = ind.get("score", 0) or ind.get("x", 0)
                rating_val = ind.get("rating", "")
            elif isinstance(ind, list) and len(ind) > 0:
                latest = ind[-1] if isinstance(ind[-1], dict) else {}
                score_val = latest.get("score", latest.get("x", 0))
                rating_val = latest.get("rating", "")
            else:
                score_val = 0
                rating_val = ""

            result["indicators"][key] = {
                "score": round(float(score_val) if score_val else 0, 1),
                "rating": rating_val,
                "name_kr": indicator_names_kr.get(key, key),
            }

        # 히스토리컬 데이터 (최근 30일)
        hist_data = raw.get("fear_and_greed_historical", {}).get("data", [])
        result["history"] = []
        for entry in hist_data[-60:]:
            result["history"].append({
                "x": entry.get("x", 0),
                "y": round(entry.get("y", 0), 1),
                "date": datetime.fromtimestamp(
                    entry.get("x", 0) / 1000, tz=timezone.utc
                ).strftime("%Y-%m-%d")
                if entry.get("x", 0) > 0
                else "",
            })

        print(f"   ✅ CNN F&G: {result['score']} ({result['rating']})")
        return result

    except Exception as e:
        print(f"   ❌ CNN F&G 실패: {e}")
        return None


# ──────────────────────────────────────────────
# 2. ApeWisdom (Reddit/4chan 핫 종목)
# ──────────────────────────────────────────────
def fetch_apewisdom():
    """ApeWisdom에서 주식 + 크립토 핫 종목 수집"""
    print("[2/4] ApeWisdom 핫 종목 수집 중...")
    results = {}
    filters = {
        "all-stocks": "주식",
        "all-crypto": "크립토",
    }

    for filter_key, label in filters.items():
        url = f"https://apewisdom.io/api/v1.0/filter/{filter_key}"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            tickers = []
            for item in data.get("results", [])[:20]:
                tickers.append({
                    "rank": item.get("rank", 0),
                    "ticker": item.get("ticker", ""),
                    "name": item.get("name", ""),
                    "mentions": int(item.get("mentions", 0)),
                    "upvotes": int(item.get("upvotes", 0)),
                    "rank_24h_ago": item.get("rank_24h_ago", 0),
                    "mentions_24h_ago": int(item.get("mentions_24h_ago", 0)),
                })

            results[filter_key] = tickers
            print(f"   ✅ ApeWisdom {label}: {len(tickers)}개 종목")

        except Exception as e:
            print(f"   ❌ ApeWisdom {label} 실패: {e}")
            results[filter_key] = []

    return results


# ──────────────────────────────────────────────
# 3. LunarCrush Public (크립토 소셜 센티먼트)
# ──────────────────────────────────────────────
def fetch_lunarcrush():
    """LunarCrush Public API에서 주요 코인 센티먼트 수집"""
    print("[3/4] LunarCrush 소셜 센티먼트 수집 중...")
    coins = ["bitcoin", "ethereum", "solana", "xrp", "dogecoin"]
    results = []

    for coin in coins:
        url = f"https://lunarcrush.com/api4/public/topic/{coin}/v1"
        try:
            resp = requests.get(url, headers=HEADERS, timeout=15)
            resp.raise_for_status()
            data = resp.json().get("data", {})

            sentiment_detail = data.get("types_sentiment_detail", {})
            total_pos = sum(
                v.get("positive", 0) for v in sentiment_detail.values() if isinstance(v, dict)
            )
            total_neg = sum(
                v.get("negative", 0) for v in sentiment_detail.values() if isinstance(v, dict)
            )
            total_neu = sum(
                v.get("neutral", 0) for v in sentiment_detail.values() if isinstance(v, dict)
            )
            total = total_pos + total_neg + total_neu

            results.append({
                "coin": coin,
                "symbol": data.get("topic", coin).upper()[:5],
                "interactions_24h": data.get("interactions_24h", 0),
                "num_contributors": data.get("num_contributors", 0),
                "num_posts": data.get("num_posts", 0),
                "sentiment_positive_pct": round(total_pos / total * 100, 1) if total > 0 else 0,
                "sentiment_negative_pct": round(total_neg / total * 100, 1) if total > 0 else 0,
                "sentiment_neutral_pct": round(total_neu / total * 100, 1) if total > 0 else 0,
                "trend": data.get("trend", "flat"),
                "types_sentiment": data.get("types_sentiment", {}),
            })
            print(f"   ✅ {coin}: 긍정 {results[-1]['sentiment_positive_pct']}%")
            time.sleep(1)  # rate limit 방지

        except Exception as e:
            print(f"   ❌ {coin} 실패: {e}")
            results.append({
                "coin": coin,
                "symbol": coin.upper()[:5],
                "error": str(e),
            })

    return results


# ──────────────────────────────────────────────
# 4. Finnhub 뉴스 센티먼트 (무료 tier)
# ──────────────────────────────────────────────
def fetch_finnhub_sentiment():
    """Finnhub에서 시장 전체 뉴스 센티먼트 수집"""
    print("[4/4] Finnhub 뉴스 센티먼트 수집 중...")
    api_key = os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        print("   ⚠️ FINNHUB_API_KEY 환경변수 없음 - 스킵")
        return None

    url = "https://finnhub.io/api/v1/news-sentiment"
    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "SPY"]
    results = []

    for symbol in symbols:
        try:
            resp = requests.get(
                url,
                params={"symbol": symbol, "token": api_key},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            sentiment = data.get("sentiment", {})
            buzz = data.get("buzz", {})

            results.append({
                "symbol": symbol,
                "bullish_pct": round(sentiment.get("bullishPercent", 0) * 100, 1),
                "bearish_pct": round(sentiment.get("bearishPercent", 0) * 100, 1),
                "sector_avg_bullish": round(
                    data.get("sectorAverageBullishPercent", 0) * 100, 1
                ),
                "articles_this_week": buzz.get("articlesInLastWeek", 0),
                "weekly_avg": buzz.get("weeklyAverage", 0),
                "buzz_score": round(buzz.get("buzz", 0), 2),
            })
            time.sleep(0.5)

        except Exception as e:
            print(f"   ⚠️ {symbol}: {e}")

    if results:
        avg_bullish = sum(r["bullish_pct"] for r in results) / len(results)
        print(f"   ✅ Finnhub: {len(results)}개 종목, 평균 강세 {avg_bullish:.1f}%")
    return results


# ──────────────────────────────────────────────
# 종합 센티먼트 점수 계산
# ──────────────────────────────────────────────
def calculate_composite_score(cnn, apewisdom, lunarcrush, finnhub):
    """각 소스를 0-100으로 정규화하고 가중 평균"""
    scores = {}
    weights = {}

    # CNN Fear & Greed (이미 0-100)
    if cnn and cnn.get("score"):
        scores["cnn_fear_greed"] = cnn["score"]
        weights["cnn_fear_greed"] = 0.35

    # LunarCrush 긍정 비율 → 0-100
    if lunarcrush:
        valid = [c for c in lunarcrush if "sentiment_positive_pct" in c and c["sentiment_positive_pct"] > 0]
        if valid:
            avg_pos = sum(c["sentiment_positive_pct"] for c in valid) / len(valid)
            # 긍정 비율 30-50% → 0-100 범위로 정규화
            scores["lunarcrush_social"] = min(100, max(0, (avg_pos - 20) * 3.33))
            weights["lunarcrush_social"] = 0.25

    # ApeWisdom 멘션 트렌드 (상승=bullish)
    if apewisdom:
        stocks = apewisdom.get("all-stocks", [])
        if stocks:
            trending_up = sum(
                1 for t in stocks[:10]
                if t.get("mentions", 0) > t.get("mentions_24h_ago", 0)
            )
            scores["apewisdom_trend"] = trending_up * 10  # 0-100
            weights["apewisdom_trend"] = 0.20

    # Finnhub 강세 비율 → 0-100
    if finnhub:
        avg_bull = sum(r["bullish_pct"] for r in finnhub) / len(finnhub)
        scores["finnhub_news"] = min(100, avg_bull * 1.5)  # 보통 40-60 범위
        weights["finnhub_news"] = 0.20

    if not scores:
        return {"score": 50, "components": {}, "rating": "데이터 없음"}

    # 가중 평균
    total_weight = sum(weights[k] for k in scores)
    composite = sum(scores[k] * weights[k] / total_weight for k in scores)
    composite = round(composite, 1)

    # 등급
    if composite >= 80:
        rating = "극도의 탐욕"
    elif composite >= 60:
        rating = "탐욕"
    elif composite >= 40:
        rating = "중립"
    elif composite >= 20:
        rating = "공포"
    else:
        rating = "극도의 공포"

    return {
        "score": composite,
        "rating": rating,
        "components": {k: round(v, 1) for k, v in scores.items()},
        "weights": weights,
    }


# ──────────────────────────────────────────────
# 메인 실행
# ──────────────────────────────────────────────
def main():
    print("=" * 50)
    print("🔍 HerdVibe 종합 센티먼트 수집기")
    print(f"⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 50)

    cnn = fetch_cnn_fear_greed()
    time.sleep(1)
    apewisdom = fetch_apewisdom()
    time.sleep(1)
    lunarcrush = fetch_lunarcrush()
    time.sleep(1)
    finnhub = fetch_finnhub_sentiment()

    composite = calculate_composite_score(cnn, apewisdom, lunarcrush, finnhub)

    output = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "composite": composite,
        "cnn_fear_greed": cnn,
        "apewisdom": apewisdom,
        "lunarcrush": lunarcrush,
        "finnhub": finnhub,
    }

    output_path = os.path.join(DATA_DIR, "sentiment.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 50)
    print(f"📊 종합 센티먼트: {composite['score']} ({composite['rating']})")
    print(f"💾 저장: {output_path}")
    print("=" * 50)

    return 0


if __name__ == "__main__":
    sys.exit(main())
