#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TVBox 源自动合并脚本 — 老年影视专用

功能：
  1. 从多个社区源汇总地址拉取原始配置
  2. 解析提取所有独立的影视源
  3. 连通性测试（并发，5秒超时），淘汰失效源
  4. 去重 + 按响应速度排序
  5. 输出 TVBox 兼容的 latest.json

作者：老年影视助手
更新：2026-10-23
"""

import json
import re
import sys
import os
import time
import asyncio
import logging
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse
from typing import List, Dict, Optional, Tuple

# 第三方依赖
import httpx

# ---------------------------------------------------------------------------
# 日志配置
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("merge_sources")

# ---------------------------------------------------------------------------
# 社区源汇总地址（我们从这些地方拉取 → 解析出独立的影视源 → 再逐个测试）
# ---------------------------------------------------------------------------
# 这些是社区公开分享的 "源列表汇总"，不是最终给 TVBox 用的单个 API
# 脚本会从每个汇总里提取出所有独立源，再逐个连通性测试
SOURCE_AGGREGATORS = [
    # Pastebin - 最稳定的社区汇总
    {
        "name": "Pastebin-gtbKvnE1",
        "url": "https://pastebin.com/raw/gtbKvnE1",
        "type": "json_with_comments",
    },
    # CH猫友配置 - 活着但主要是 type 3 (CSP) 源
    {
        "name": "CH猫友-meowcf",
        "url": "https://freed.yuanhsing.cf/TVBox/meowcf.json",
        "type": "json_with_comments",
    },
    # 如果未来发现新的汇总源，加在这里即可
]

# 输出文件路径（相对于仓库根目录）
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "sources"
OUTPUT_FILE = OUTPUT_DIR / "latest.json"

# 连通性测试配置
TEST_TIMEOUT = 5.0        # 每个源 5 秒超时
TEST_CONCURRENCY = 20     # 并发 20 个测试
MIN_SOURCES_KEEP = 5      # 最少保留 5 个源（全挂了也得有兜底）


# ===========================================================================
# 第一步：从汇总地址拉取原始配置
# ===========================================================================

def strip_json_comments(raw: str) -> str:
    """
    社区 TVBox 配置经常带有 // 注释，JSON 标准不允许。
    这里把 // 注释整行去掉，保留有效 JSON。
    """
    lines = []
    for line in raw.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("//") or stripped.startswith("#"):
            continue
        lines.append(line)
    return "\n".join(lines)


async def fetch_aggregator(client: httpx.AsyncClient, agg: Dict) -> List[Dict]:
    """从一个汇总地址拉取并解析出源列表"""
    url = agg["url"]
    name = agg["name"]

    try:
        resp = await client.get(url, timeout=15.0)
        resp.raise_for_status()
        raw = resp.text
    except Exception as e:
        log.warning(f"  [跳过] {name}: 拉取失败 - {e}")
        return []

    # 清理注释
    cleaned = strip_json_comments(raw)

    # 尝试 JSON 解析
    try:
        data = json.loads(cleaned)
    except json.JSONDecodeError as e:
        # 有些汇总源是 txt 格式（每个源一行 URL），跳过
        log.warning(f"  [跳过] {name}: JSON 解析失败 - {e}")
        return []

    # 提取 sites 数组
    if isinstance(data, dict) and "sites" in data:
        sites = data["sites"]
    elif isinstance(data, list):
        sites = data
    else:
        log.warning(f"  [跳过] {name}: 找不到 sites 字段")
        return []

    log.info(f"  [OK] {name}: 提取到 {len(sites)} 个源")
    return sites


# ===========================================================================
# 第二步：合并 + 去重
# ===========================================================================

def normalize_source(site: Dict, source_aggregator: str) -> Optional[Dict]:
    """把各种格式的源统一成 TVBox 标准格式"""
    if not isinstance(site, dict):
        return None

    api = site.get("api", "")
    if not api or not isinstance(api, str):
        return None

    # 跳过 URL 还是占位符的
    if api in ("csp", "csp_LiteApple", "csp_Kunyu77"):
        # 这些是 TVBox 内置 CSP 插件标识，没有独立 API URL
        # type=3 的 CSP 源可以保留，但 type=1 的必须有真实 API URL
        stype = site.get("type", 3)
        if stype != 3:
            return None
        # CSP 源特殊处理：api 是插件名不是 URL，但 TVBox 能识别
        pass

    key = site.get("key") or site.get("name") or api
    name = site.get("name") or key

    return {
        "key": str(key).strip(),
        "name": str(name).strip(),
        "api": api.strip(),
        "type": int(site.get("type", 1)),
        "searchable": int(site.get("searchable", 1)),
        "quickSearch": int(site.get("quickSearch", 1)),
        "filterable": int(site.get("filterable", 0)),
        "playUrl": site.get("playUrl", ""),
        "ext": site.get("ext", ""),
        "_source_aggregator": source_aggregator,
    }


def merge_and_deduplicate(all_sources: List[Dict]) -> List[Dict]:
    """按 API URL 去重"""
    seen = {}
    for s in all_sources:
        api_key = s["api"]
        # 同 API URL 的只保留第一个出现的
        if api_key not in seen:
            seen[api_key] = s
    return list(seen.values())


# ===========================================================================
# 第三步：连通性测试（并发）
# ===========================================================================

async def test_source(
    client: httpx.AsyncClient, source: Dict, semaphore: asyncio.Semaphore
) -> Tuple[Dict, bool, float]:
    """测试单个源的 API 是否可达"""
    api = source["api"]

    # CSP 源（type=3）的 api 不是 URL，无法 HTTP 测试，直接认为可用
    if source["type"] == 3:
        return (source, True, 0.0)

    async with semaphore:
        start = time.time()
        try:
            # 发一个最轻量的请求：搜索 "西游记" 测试接口是否响应
            test_url = api
            if "?" not in test_url:
                test_url += "?wd=%E8%A5%BF%E6%B8%B8%E8%AE%B0"
            else:
                test_url += "&wd=%E8%A5%BF%E6%B8%B8%E8%AE%B0"

            resp = await client.get(test_url, timeout=TEST_TIMEOUT)
            elapsed = time.time() - start

            # 检查响应有效性
            if resp.status_code == 200 and len(resp.text) > 10:
                return (source, True, elapsed)
            else:
                return (source, False, elapsed)

        except Exception:
            elapsed = time.time() - start
            return (source, False, elapsed)


async def run_connectivity_test(sources: List[Dict]) -> Tuple[List[Dict], int]:
    """并发测试所有源，返回（可用源列表, 测试总数）"""
    sem = asyncio.Semaphore(TEST_CONCURRENCY)
    async with httpx.AsyncClient(follow_redirects=True) as client:
        tasks = [test_source(client, s, sem) for s in sources]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    alive = []
    dead_count = 0
    for r in results:
        if isinstance(r, Exception):
            dead_count += 1
            continue
        source, is_alive, elapsed = r
        if is_alive:
            source["_response_time"] = round(elapsed, 2)
            alive.append(source)
        else:
            dead_count += 1

    log.info(f"  连通性测试: {len(sources) - dead_count}/{len(sources)} 存活")
    return alive, len(sources)


# ===========================================================================
# 第四步：排序 + 输出
# ===========================================================================

def sort_sources(sources: List[Dict]) -> List[Dict]:
    """排序策略：
    1. CSP 源 (type=3) 排后面（数量少，TVBox 内置插件依赖）
    2. API 源 (type=1) 按响应速度从快到慢
    """
    api_sources = [s for s in sources if s["type"] != 3]
    csp_sources = [s for s in sources if s["type"] == 3]

    # API 源按响应速度排序（越快越靠前）
    api_sources.sort(key=lambda s: s.get("_response_time", 999))

    return api_sources + csp_sources


def build_output(alive_sources: List[Dict]) -> Dict:
    """构建 TVBox 兼容的输出 JSON，去掉内部字段"""
    clean_sites = []
    for s in alive_sources:
        clean = {k: v for k, v in s.items() if not k.startswith("_")}
        clean_sites.append(clean)

    # 兜底：如果全挂了，至少给一个示例源
    if len(clean_sites) < MIN_SOURCES_KEEP:
        log.warning(f"存活源不足 {MIN_SOURCES_KEEP}，加入兜底源")
        clean_sites = clean_sites + [
            {
                "key": "douban_movie",
                "name": "豆瓣电影API(兜底)",
                "type": 1,
                "api": "https://frodo.douban.com/api/v2/",
                "searchable": 0,
                "quickSearch": 0,
            }
        ]

    return {
        "_meta": {
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_sources": len(clean_sites),
            "note": "老年影视专用 - 每天自动更新",
        },
        "sites": clean_sites,
        "lives": [
            {
                "group": "CCTV直播",
                "channels": [
                    {
                        "name": "CCTV1综合",
                        "urls": ["https://live.cctv.com/live/cctv1.m3u8"],
                    },
                    {
                        "name": "CCTV2财经",
                        "urls": ["https://live.cctv.com/live/cctv2.m3u8"],
                    },
                    {
                        "name": "CCTV3综艺",
                        "urls": ["https://live.cctv.com/live/cctv3.m3u8"],
                    },
                    {
                        "name": "CCTV4中文国际",
                        "urls": ["https://live.cctv.com/live/cctv4.m3u8"],
                    },
                    {
                        "name": "CCTV5体育",
                        "urls": ["https://live.cctv.com/live/cctv5.m3u8"],
                    },
                    {
                        "name": "CCTV6电影",
                        "urls": ["https://live.cctv.com/live/cctv6.m3u8"],
                    },
                    {
                        "name": "CCTV7国防军事",
                        "urls": ["https://live.cctv.com/live/cctv7.m3u8"],
                    },
                    {
                        "name": "CCTV8电视剧",
                        "urls": ["https://live.cctv.com/live/cctv8.m3u8"],
                    },
                    {
                        "name": "CCTV13新闻",
                        "urls": ["https://live.cctv.com/live/cctv13.m3u8"],
                    },
                ],
            }
        ],
    }


# ===========================================================================
# 主流程
# ===========================================================================

async def main_async():
    log.info("=" * 60)
    log.info("TVBox 源自动合并脚本启动")
    log.info("=" * 60)

    # 1. 从所有汇总地址拉取
    log.info(f"\n[1/4] 拉取 {len(SOURCE_AGGREGATORS)} 个社区汇总...")
    all_raw_sites = []
    async with httpx.AsyncClient(timeout=30.0) as client:
        for agg in SOURCE_AGGREGATORS:
            sites = await fetch_aggregator(client, agg)
            all_raw_sites.extend([(s, agg["name"]) for s in sites])

    if not all_raw_sites:
        log.error("所有汇总地址都拉取失败！检查网络或换源")
        sys.exit(1)

    # 2. 规范化 + 去重
    log.info(f"\n[2/4] 规范化 + 去重 {len(all_raw_sites)} 个原始源...")
    normalized = []
    for raw, agg_name in all_raw_sites:
        n = normalize_source(raw, agg_name)
        if n:
            normalized.append(n)

    unique_sources = merge_and_deduplicate(normalized)
    log.info(f"  去重后剩余 {len(unique_sources)} 个独立源")

    # 3. 连通性测试
    log.info(f"\n[3/4] 并发连通性测试 ({TEST_CONCURRENCY}路并发, {TEST_TIMEOUT}s超时)...")
    alive, tested = await run_connectivity_test(unique_sources)

    if not alive:
        log.warning("所有源都测不通了，直接用去重后的列表输出，让 TVBox 自己尝试")
        alive = unique_sources

    # 4. 排序 + 输出
    log.info(f"\n[4/4] 排序 + 写入输出文件...")
    sorted_sources = sort_sources(alive)
    output = build_output(sorted_sources)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    # 打印结果摘要
    meta = output["_meta"]
    log.info(f"\n{'=' * 60}")
    log.info(f"✅ 完成！输出: {OUTPUT_FILE}")
    log.info(f"   时间: {meta['generated_at']}")
    log.info(f"   可用源数: {meta['total_sources']}")
    log.info(f"{'=' * 60}")

    # 打印最快的前 5 个源
    top5 = [s for s in sorted_sources if s["type"] != 3][:5]
    if top5:
        log.info(f"\n🏆 最快的 5 个 API 源:")
        for i, s in enumerate(top5, 1):
            rt = s.get("_response_time", "N/A")
            log.info(f"  {i}. [{rt}s] {s['name']} → {s['api'][:60]}...")

    return meta["total_sources"]


def main():
    return asyncio.run(main_async())


if __name__ == "__main__":
    count = main()
    sys.exit(0 if count > 0 else 1)
