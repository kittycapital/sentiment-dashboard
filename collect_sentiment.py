#!/usr/bin/env python3
"""
HerdVibe 종합 센티먼트 수집기 v3
무료 API만 사용 (키 불필요): CNN F&G, ApeWisdom, Alternative.me, CoinGecko, Finnhub(옵션)
"""

import json, os, sys, time
from datetime import datetime, timezone
import requests

DATA_DIR = os.environ.get("DATA_DIR", "data")
os.makedirs(DATA_DIR, exist_ok=True)
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}


def safe_int(val):
    try: return int(val) if val is not None else 0
    except (ValueError, TypeError): return 0


def safe_float(val, default=0):
    try: return float(val) if val is not None else default
    except (ValueError, TypeError): return default


# ──────────────────────────────────────────────
# 1. CNN Fear & Greed (주식) — 날짜 없이 호출
# ──────────────────────────────────────────────
def fetch_cnn_fear_greed():
    print("[1/4] CNN Fear & Greed Index 수집 중...")
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    try:
        resp = requests.get(url, headers=UA, timeout=20)
        resp.raise_for_status()
        raw = resp.json()

        fg = raw.get("fear_and_greed", {})
        result = {
            "score": round(safe_float(fg.get("score")), 1),
            "rating": fg.get("rating", ""),
            "previous_close": round(safe_float(fg.get("previous_close")), 1),
            "week_ago": round(safe_float(fg.get("previous_1_week")), 1),
            "month_ago": round(safe_float(fg.get("previous_1_month")), 1),
            "year_ago": round(safe_float(fg.get("previous_1_year")), 1),
            "indicators": {},
        }

        # 7개 하위 지표 — 최상위 레벨 키
        indicator_map = {
            "market_momentum_sp500": "주가 모멘텀",
            "stock_price_strength": "주가 강도",
            "stock_price_breadth": "주가 폭",
            "put_call_options": "풋/콜 옵션",
            "market_volatility_vix": "시장 변동성(VIX)",
            "safe_haven_demand": "안전자산 수요",
            "junk_bond_demand": "정크본드 수요",
        }
        for key, name_kr in indicator_map.items():
            ind = raw.get(key, {})
            if isinstance(ind, dict):
                result["indicators"][key] = {
                    "score": round(safe_float(ind.get("score")), 1),
                    "rating": ind.get("rating", ""),
                    "name_kr": name_kr,
                }
            else:
                result["indicators"][key] = {"score": 0, "rating": "", "name_kr": name_kr}

        # 히스토리컬 — CNN이 주는 것 + 기존 누적
        hist_raw = raw.get("fear_and_greed_historical", {})
        hist_data = hist_raw.get("data", []) if isinstance(hist_raw, dict) else []
        new_history = {}
        for entry in hist_data:
            ts = entry.get("x", 0)
            val = entry.get("y")
            if ts > 0 and val is not None:
                d = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
                new_history[d] = round(safe_float(val), 1)

        # 기존 히스토리 병합
        existing_path = os.path.join(DATA_DIR, "sentiment.json")
        if os.path.exists(existing_path):
            try:
                with open(existing_path, "r", encoding="utf-8") as f:
                    existing = json.load(f)
                for h in existing.get("cnn_fear_greed", {}).get("history", []):
                    d = h.get("date", "")
                    if d and d not in new_history:
                        new_history[d] = h.get("y", 0)
            except Exception:
                pass

        result["history"] = [{"date": d, "y": v} for d, v in sorted(new_history.items())]
        print(f"   ✅ CNN F&G: {result['score']} ({result['rating']}), 히스토리 {len(result['history'])}일")
        for v in result["indicators"].values():
            print(f"      {v['name_kr']}: {v['score']} ({v['rating']})")
        return result

    except Exception as e:
        print(f"   ❌ CNN F&G 실패: {e}")
        return None


# ──────────────────────────────────────────────
# 2. ApeWisdom (Reddit/4chan 핫 종목) — None 값 처리
# ──────────────────────────────────────────────
def fetch_apewisdom():
    print("[2/4] ApeWisdom 핫 종목 수집 중...")
    results = {}
    for filter_key, label in [("all-stocks", "주식"), ("all-crypto", "크립토")]:
        url = f"https://apewisdom.io/api/v1.0/filter/{filter_key}"
        try:
            resp = requests.get(url, headers=UA, timeout=15)
            resp.raise_for_status()
            data = resp.json()
            tickers = []
            for item in data.get("results", [])[:20]:
                tickers.append({
                    "rank": safe_int(item.get("rank")),
                    "ticker": item.get("ticker", "").replace(".X", ""),
                    "name": item.get("name", ""),
                    "mentions": safe_int(item.get("mentions")),
                    "upvotes": safe_int(item.get("upvotes")),
                    "rank_24h_ago": safe_int(item.get("rank_24h_ago")),
                    "mentions_24h_ago": safe_int(item.get("mentions_24h_ago")),
                })
            results[filter_key] = tickers
            print(f"   ✅ ApeWisdom {label}: {len(tickers)}개 종목")
        except Exception as e:
            print(f"   ❌ ApeWisdom {label} 실패: {e}")
            results[filter_key] = []
    return results


# ──────────────────────────────────────────────
# 3. 크립토 센티먼트 (Alternative.me F&G + CoinGecko 트렌딩)
#    둘 다 완전 무료, 키 불필요
# ──────────────────────────────────────────────
def fetch_crypto_sentiment():
    print("[3/4] 크립토 센티먼트 수집 중...")
    result = {"fear_greed": None, "trending": []}

    # Alternative.me Crypto Fear & Greed — 30일치
    try:
        resp = requests.get("https://api.alternative.me/fng/?limit=30&format=json", headers=UA, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("data", [])
        if data:
            latest = data[0]
            result["fear_greed"] = {
                "value": safe_int(latest.get("value")),
                "classification": latest.get("value_classification", ""),
                "timestamp": latest.get("timestamp", ""),
            }
            result["fear_greed_history"] = [
                {"date": datetime.fromtimestamp(int(d.get("timestamp", 0)), tz=timezone.utc).strftime("%Y-%m-%d"),
                 "value": safe_int(d.get("value")),
                 "classification": d.get("value_classification", "")}
                for d in data if d.get("timestamp")
            ]
            print(f"   ✅ 크립토 F&G: {result['fear_greed']['value']} ({result['fear_greed']['classification']})")
    except Exception as e:
        print(f"   ❌ Alternative.me 실패: {e}")

    # CoinGecko 트렌딩 코인 (무료, 키 불필요)
    try:
        resp = requests.get("https://api.coingecko.com/api/v3/search/trending", headers=UA, timeout=10)
        resp.raise_for_status()
        coins = resp.json().get("coins", [])
        result["trending"] = []
        for c in coins[:10]:
            item = c.get("item", {})
            result["trending"].append({
                "name": item.get("name", ""),
                "symbol": item.get("symbol", ""),
                "market_cap_rank": item.get("market_cap_rank", 0),
                "score": item.get("score", 0),
                "price_btc": item.get("price_btc", 0),
            })
        print(f"   ✅ CoinGecko 트렌딩: {len(result['trending'])}개 코인")
    except Exception as e:
        print(f"   ❌ CoinGecko 실패: {e}")

    return result


# ──────────────────────────────────────────────
# 4. Finnhub (옵션 — API 키 있을 때만)
# ──────────────────────────────────────────────
def fetch_finnhub_sentiment():
    print("[4/4] Finnhub 뉴스 센티먼트 수집 중...")
    api_key = os.environ.get("FINNHUB_API_KEY", "")
    if not api_key:
        print("   ⚠️ FINNHUB_API_KEY 없음 — 스킵 (finnhub.io에서 무료 키 발급)")
        return None

    symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "NVDA", "META", "SPY"]
    results = []
    for symbol in symbols:
        try:
            resp = requests.get("https://finnhub.io/api/v1/news-sentiment",
                                params={"symbol": symbol, "token": api_key}, timeout=10)
            resp.raise_for_status()
            data = resp.json()
            sentiment = data.get("sentiment", {})
            buzz = data.get("buzz", {})
            results.append({
                "symbol": symbol,
                "bullish_pct": round(safe_float(sentiment.get("bullishPercent")) * 100, 1),
                "bearish_pct": round(safe_float(sentiment.get("bearishPercent")) * 100, 1),
                "articles_this_week": safe_int(buzz.get("articlesInLastWeek")),
                "buzz_score": round(safe_float(buzz.get("buzz")), 2),
            })
            time.sleep(0.5)
        except Exception as e:
            print(f"   ⚠️ {symbol}: {e}")
    if results:
        print(f"   ✅ Finnhub: {len(results)}개 종목")
    return results


# ──────────────────────────────────────────────
# 종합 점수
# ──────────────────────────────────────────────
def calculate_composite(cnn, apewisdom, crypto, finnhub):
    scores, weights = {}, {}

    if cnn and cnn.get("score"):
        scores["cnn_fear_greed"] = cnn["score"]
        weights["cnn_fear_greed"] = 0.35

    if crypto and crypto.get("fear_greed"):
        scores["crypto_fear_greed"] = safe_float(crypto["fear_greed"]["value"])
        weights["crypto_fear_greed"] = 0.25

    if apewisdom:
        stocks = apewisdom.get("all-stocks", [])
        if stocks:
            up = sum(1 for t in stocks[:10] if t["mentions"] > t["mentions_24h_ago"])
            scores["community_trend"] = up * 10
            weights["community_trend"] = 0.20

    if finnhub:
        avg = sum(r["bullish_pct"] for r in finnhub) / len(finnhub)
        scores["news_sentiment"] = min(100, avg * 1.5)
        weights["news_sentiment"] = 0.20

    if not scores:
        return {"score": 50, "components": {}, "rating": "데이터 없음"}

    tw = sum(weights[k] for k in scores)
    comp = round(sum(scores[k] * weights[k] / tw for k in scores), 1)

    if comp >= 80: r = "극도의 탐욕"
    elif comp >= 60: r = "탐욕"
    elif comp >= 40: r = "중립"
    elif comp >= 20: r = "공포"
    else: r = "극도의 공포"

    return {"score": comp, "rating": r,
            "components": {k: round(v, 1) for k, v in scores.items()}, "weights": weights}


def main():
    print("=" * 50)
    print("🔍 HerdVibe 종합 센티먼트 수집기 v3")
    print(f"⏰ {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 50)

    cnn = fetch_cnn_fear_greed(); time.sleep(1)
    apewisdom = fetch_apewisdom(); time.sleep(1)
    crypto = fetch_crypto_sentiment(); time.sleep(1)
    finnhub = fetch_finnhub_sentiment()

    composite = calculate_composite(cnn, apewisdom, crypto, finnhub)

    output = {
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "composite": composite,
        "cnn_fear_greed": cnn,
        "apewisdom": apewisdom,
        "crypto_sentiment": crypto,
        "finnhub": finnhub,
    }

    path = os.path.join(DATA_DIR, "sentiment.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n📊 종합: {composite['score']} ({composite['rating']})")
    print(f"💾 저장: {path}")


if __name__ == "__main__":
    main()
