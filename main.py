"""自主刷 B 站 v3：像人一样逛——首页盲刷 / 搜自己的兴趣 / 追某个 UP，
点开后看简介、热评、弹幕和封面，看完才说话；会记梗、记口味、记划走。"""

import os
import asyncio
import hashlib
import json
import random
import sqlite3
import subprocess
import time
import urllib.parse
from datetime import datetime, timedelta
from pathlib import Path

import aiohttp

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star, register
from astrbot.core.message.message_event_result import MessageChain

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
HEADERS = {"User-Agent": UA, "Referer": "https://www.bilibili.com/", "Origin": "https://www.bilibili.com"}

MIXIN = [46, 47, 18, 2, 53, 8, 23, 32, 15, 50, 10, 31, 58, 3, 45, 35,
         27, 43, 5, 49, 33, 9, 42, 19, 29, 28, 14, 39, 12, 38, 41, 13,
         37, 48, 7, 16, 24, 55, 40, 61, 26, 17, 0, 1, 60, 51, 30, 4,
         22, 25, 54, 21, 56, 59, 6, 63, 57, 62, 11, 36, 20, 34, 44, 52]
BAD = chr(33) + chr(39) + "()*"

TARGETS = [t for t in os.getenv("BILI_DRIFT_TARGETS", "").split(",") if t]
MIN_GAP = 1800
MAX_GAP = 7200
SKIP_CHANCE = 0.12
START_HOUR = 9
END_HOUR = 23.5
MAX_WORDS = 40
FEED_SIZE = 15
COMMENT_N = 6
DANMAKU_N = 14
SEE_COVER_CHANCE = 0.6
SCROLL_AWAY_CHANCE = 0.25
STAY_TUNE_CHANCE = 0.2
LURK_CHANCE = 0.1
MAX_CHAIN = 2
WATCH_CHANCE = 0.3
WATCH_MAX_SEC = 900
WATCH_FILE = "/tmp/bili_drift_watch.mp4"

DB_PATH = Path("/AstrBot/data/data_v4.db")
STATE_PATH = Path("/AstrBot/data/plugin_data/bili_drift.json")
MEME_PATH = Path("/AstrBot/data/plugin_data/bili_drift_memes.md")
FIRE_PATH = Path("/AstrBot/data/plugin_data/bili_drift_fire")


async def _json_get(sess, url, params=None):
    async with sess.get(url, params=params) as resp:
        return await resp.json(content_type=None)


async def _wbi_key(sess):
    d = await _json_get(sess, "https://api.bilibili.com/x/web-interface/nav")
    w = ((d.get("data") or {}).get("wbi_img") or {})
    img = str(w.get("img_url") or "").rsplit("/", 1)[-1].split(".")[0]
    sub = str(w.get("sub_url") or "").rsplit("/", 1)[-1].split(".")[0]
    if not img or not sub:
        return None
    return "".join((img + sub)[i] for i in MIXIN)[:32]


async def _sign(sess, params):
    mixin = await _wbi_key(sess)
    p = dict(params)
    p["wts"] = int(time.time())
    out = {}
    for k in sorted(p.keys()):
        out[k] = "".join(ch for ch in str(p[k]) if ch not in BAD)
    q = urllib.parse.urlencode(out)
    out["w_rid"] = hashlib.md5((q + (mixin or "")).encode()).hexdigest()
    return out


def _num(v):
    try:
        n = int(v or 0)
    except Exception:
        return "0"
    if n >= 100000000:
        return "%.1f亿" % (n / 100000000.0)
    if n >= 10000:
        return "%.1f万" % (n / 10000.0)
    return str(n)


def _dur(v):
    try:
        n = int(v or 0)
    except Exception:
        return ""
    m, s = divmod(n, 60)
    h, m = divmod(m, 60)
    if h:
        return "%d:%02d:%02d" % (h, m, s)
    return "%d:%02d" % (m, s)


def _flat(t, n=200):
    s = str(t or "").replace(chr(10), " ").replace(chr(13), " ").strip()
    return s if len(s) <= n else s[:n] + "…"


def _tag(s, prefix_len):
    return s.replace(":", "", 1).replace("：", "", 1)[prefix_len:].strip()


def _split_tags(s):
    raw = _tag(s, 4)
    for sep in ("、", "，", ",", "/", "；", ";", " "):
        raw = raw.replace(sep, "|")
    return [p.strip()[:16] for p in raw.split("|") if p.strip()]


@register(
    "astrbot_plugin_bili_drift",
    "小肥鱼",
    "自主刷B站v3：逛首页/搜兴趣/追UP，看简介热评弹幕和封面，看完才说话",
    "3.0.0",
)
class BiliDriftPlugin(Star):
    def __init__(self, context: Context, config: dict | None = None):
        super().__init__(context)
        self.context = context
        self.conf = config or {}
        self._task = None
        self._session = None
        self._busy = False
        self._tune_next = 0.0

    async def initialize(self) -> None:
        self._task = asyncio.create_task(self._loop())
        logger.info("[bili_drift] v3 启动：%d-%d 秒一轮" % (MIN_GAP, MAX_GAP))

    async def terminate(self) -> None:
        if self._task:
            self._task.cancel()
            self._task = None
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    async def _sess(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=HEADERS,
                                                 timeout=aiohttp.ClientTimeout(total=20))
        return self._session

    def _state(self) -> dict:
        try:
            st = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            st = {}
        for k in ("seen", "likes", "hates", "recent_up", "scrolled"):
            st.setdefault(k, [])
        return st

    def _save_state(self, st: dict) -> None:
        try:
            STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
            st["seen"] = st.get("seen", [])[-150:]
            st["scrolled"] = st.get("scrolled", [])[-150:]
            st["likes"] = st.get("likes", [])[-12:]
            st["hates"] = st.get("hates", [])[-12:]
            st["recent_up"] = st.get("recent_up", [])[-10:]
            STATE_PATH.write_text(json.dumps(st, ensure_ascii=False), encoding="utf-8")
        except Exception as exc:
            logger.warning("[bili_drift] 状态写入失败: %s" % exc)

    async def _feed(self, n: int = FEED_SIZE):
        sess = await self._sess()
        params = await _sign(sess, {"ps": n, "fresh_type": 4, "feed_version": "V8",
                                     "fresh_idx": random.randint(1, 8), "fresh_idx_1h": 1})
        d = await _json_get(sess, "https://api.bilibili.com/x/web-interface/wbi/index/top/feed/rcmd", params)
        if d.get("code") != 0:
            raise RuntimeError("推荐流 code=%s" % d.get("code"))
        return ((d.get("data") or {}).get("item") or [])[:n]

    async def _search(self, keyword: str, n: int = 10):
        sess = await self._sess()
        params = await _sign(sess, {"search_type": "video", "keyword": keyword, "page": 1,
                                     "page_size": n, "order": "total"})
        d = await _json_get(sess, "https://api.bilibili.com/x/web-interface/wbi/search/type", params)
        if d.get("code") != 0:
            return []
        out = []
        for it in ((d.get("data") or {}).get("result") or [])[:n]:
            out.append({"bvid": it.get("bvid"), "title": it.get("title"),
                        "owner": {"name": it.get("author")}, "tname": "",
                        "duration": 0, "pic": it.get("pic"), "desc": it.get("description")})
        return out

    async def _dynamic(self, uid: str, n: int = 10):
        sess = await self._sess()
        url = "https://api.bilibili.com/x/polymer/web-dynamic/v1/feed/space"
        d = await _json_get(sess, url, {"host_mid": uid, "features": "itemOpusStyle,listOnlyfans,opusBigCover,onlyfansVote"})
        if d.get("code") != 0:
            return []
        out = []
        for it in ((d.get("data") or {}).get("items") or [])[:n]:
            major = (((it.get("modules") or {}).get("module_dynamic") or {}).get("major") or {})
            arc = major.get("archive") or {}
            if arc.get("bvid"):
                out.append({"bvid": arc.get("bvid"), "title": arc.get("title"),
                            "owner": {"name": (arc.get("owner") or {}).get("name")},
                            "tname": "", "duration": 0, "pic": arc.get("cover"), "desc": ""})
        return out

    async def _detail(self, bvid: str):
        sess = await self._sess()
        d = await _json_get(sess, "https://api.bilibili.com/x/web-interface/view", {"bvid": bvid})
        if d.get("code") != 0:
            return None
        return d.get("data") or None

    async def _comments(self, aid, n: int = COMMENT_N):
        if not aid:
            return []
        sess = await self._sess()
        d = await _json_get(sess, "https://api.bilibili.com/x/v2/reply",
                            {"type": 1, "oid": aid, "sort": 2, "ps": n, "pn": 1})
        if d.get("code") != 0:
            return []
        out = []
        for r in ((d.get("data") or {}).get("replies") or [])[:n]:
            who = ((r.get("member") or {}).get("uname")) or "匿名"
            txt = _flat((r.get("content") or {}).get("message"), 70)
            if txt:
                out.append(who + "：" + txt)
        return out

    async def _danmaku(self, cid, n: int = DANMAKU_N):
        if not cid:
            return []
        sess = await self._sess()
        url = "https://comment.bilibili.com/" + str(cid) + ".xml"
        try:
            async with sess.get(url) as resp:
                if resp.status != 200:
                    return []
                raw = await resp.text(errors="ignore")
        except Exception:
            return []
        texts = []
        for seg in raw.split("<d p=")[1:]:
            i = seg.find(">")
            j = seg.find("</d>")
            if i < 0 or j <= i:
                continue
            t = _flat(seg[i + 1:j], 50)
            if t:
                texts.append(t)
        if len(texts) > n:
            texts = random.sample(texts, n)
        return texts

    async def _cover(self, url) -> str | None:
        u = str(url or "").strip()
        if not u:
            return None
        if u.startswith("//"):
            u = "https:" + u
        path = Path("/tmp/bili_drift_cover.jpg")
        try:
            sess = await self._sess()
            async with sess.get(u) as resp:
                if resp.status != 200:
                    return None
                data = await resp.read()
            if len(data) < 3000:
                return None
            path.write_bytes(data)
            return str(path)
        except Exception as exc:
            logger.warning("[bili_drift] 封面下载失败: %s" % exc)
            return None

    async def _watch(self, bvid, context: str = "") -> str:
        """真看视频：下载 → 抽帧 → 多模态。失败返回空串。"""
        if not bvid:
            return ""
        url = "https://www.bilibili.com/video/" + str(bvid)
        try:
            proc = await asyncio.create_subprocess_exec(
                "yt-dlp", "--no-warnings", "-f", "bv*[height<=480]+ba/b[height<=480]/b",
                "--merge-output-format", "mp4", "-o", WATCH_FILE, url,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
            try:
                _, err = await asyncio.wait_for(proc.communicate(), timeout=180)
            except asyncio.TimeoutError:
                proc.kill()
                logger.warning("[bili_drift] 下载超时")
                return ""
            if proc.returncode != 0:
                logger.warning("[bili_drift] 下载失败: " + err.decode("utf-8", "ignore")[-120:])
                return ""
        except Exception as exc:
            logger.warning("[bili_drift] 下载异常: %s" % exc)
            return ""
        try:
            import importlib.util
            path = "/AstrBot/data/plugins/astrbot_plugin_videofetch/video_understand.py"
            spec = importlib.util.spec_from_file_location("bili_drift_vu", path)
            mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(mod)
            desc = await mod.understand(WATCH_FILE, "这条视频到底在演什么？如果是梗或名场面，点出是什么梗、梗点在哪。看不出来就说看不出来。", context)
            desc = (desc or "").strip()
            for pre in ("这两句话", "这段话", "以下是", "概括", "总结"):
                if pre in desc[:24]:
                    i = max(desc.find("："), desc.find(":"))
                    if 0 <= i < 40:
                        desc = desc[i + 1:].strip()
                    break
            if desc:
                logger.info("[bili_drift] 看完视频：%s" % desc[:60])
            return desc
        except Exception as exc:
            logger.warning("[bili_drift] 看视频失败: %s" % exc)
            return ""

    def _persona(self) -> str:
        try:
            con = sqlite3.connect("file:" + str(DB_PATH) + "?mode=ro", uri=True)
            row = con.execute("select system_prompt from personas where persona_id=?", ("小肥鱼",)).fetchone()
            con.close()
            return ((row[0] if row else "") or "")[:4200]
        except Exception as exc:
            logger.warning("[bili_drift] 读人格失败: %s" % exc)
            return "你是 QQ 里的女生小肥鱼，说话短、有脾气、不客套。"

    async def _llm(self, prompt: str, image: str | None = None, max_len: int = 700) -> str:
        try:
            prov = self.context.get_using_provider()
        except Exception:
            prov = None
        if prov is None:
            return ""
        acc = ""
        try:
            if image:
                gen = prov.text_chat_stream(prompt=prompt, system_prompt=self._persona(),
                                            image_urls=[image])
            else:
                gen = prov.text_chat_stream(prompt=prompt, system_prompt=self._persona())
            async for chunk in gen:
                t = getattr(chunk, "completion_text", "") or ""
                if not t:
                    continue
                acc = t if t.startswith(acc) else acc + t
        except Exception as exc:
            logger.error("[bili_drift] LLM 失败: %s" % exc)
            if image:
                return await self._llm(prompt, None, max_len)
            return ""
        return acc.strip()[:max_len]

    async def _choose(self, pool, st: dict, mood: str) -> int:
        lines = []
        for i, it in enumerate(pool, 1):
            owner = (it.get("owner") or {}).get("name") or "?"
            lines.append(str(i) + ". " + _flat(it.get("title"), 46) + " | " + str(owner)
                         + " | " + str(it.get("tname") or "") + " | " + _dur(it.get("duration")))
        taste = ""
        if st.get("likes"):
            taste = taste + "你平时爱看：" + "、".join(st["likes"][-6:]) + "。"
        if st.get("hates"):
            taste = taste + "你不太爱看：" + "、".join(st["hates"][-6:]) + "。"
        prompt = "你在家闲着刷 B 站，" + mood + "，刷出这 " + str(len(pool)) + " 条：" + chr(10)
        prompt = prompt + chr(10).join(lines) + chr(10) + chr(10) + taste + chr(10)
        prompt = prompt + "只看标题和 UP 主，挑一条你最想点开看的；一条都不想点就回 0。"
        prompt = prompt + "只回一个数字。"
        ans = await self._llm(prompt, None, 12)
        num = ""
        for ch in ans:
            if ch.isdigit():
                num = num + ch
            elif num:
                break
        if not num.isdigit():
            return -1
        idx = int(num)
        if idx < 1 or idx > len(pool):
            return 0
        return idx

    async def _speak(self, pick, detail, comments, danmaku, image, watch, st: dict) -> dict:
        stat = (detail or {}).get("stat") or {}
        owner = (detail or {}).get("owner") or {}
        head = ["标题：" + _flat((detail or {}).get("title") or pick.get("title"), 80),
                "UP：" + str(owner.get("name") or ((pick.get("owner") or {}).get("name")) or "?"),
                "分区：" + str((detail or {}).get("tname") or pick.get("tname") or "?"),
                "数据：播放" + _num(stat.get("view")) + " 点赞" + _num(stat.get("like"))
                + " 弹幕" + _num(stat.get("danmaku")) + " 时长" + _dur((detail or {}).get("duration")),
                "简介：" + _flat((detail or {}).get("desc") or pick.get("desc"), 200)]
        body = [chr(10).join(head)]
        if comments:
            body.append("热评：" + chr(10) + chr(10).join(comments))
        if danmaku:
            body.append("弹幕飘过：" + " / ".join(danmaku))
        if image:
            body.append("（附了一张封面图，你看看画面）")
        if watch:
            body.append("你看完视频之后的印象：" + str(watch)[:400])
        rules = ["你刚把这条点开看完（简介、评论、弹幕、封面都看了）。用你自己的话跟我说一句，不超过 "
                 + str(MAX_WORDS) + " 字，一句话，不列点，不解释你在干什么，不要客套收尾。",
                 "如果这条是个梗、值得记下来，另起一行写 MEMO: 一句话说清这个梗。",
                 "如果这对题材很有感或有反感，再另起一行写 LIKE: 标签 或 HATE: 标签。",
                 "第一行只放你要说的那句话。"]
        prompt = chr(10).join(body) + chr(10) + chr(10) + chr(10).join(rules)
        raw = await self._llm(prompt, image, 700)
        text = ""
        memo = ""
        likes = []
        hates = []
        for line in raw.split(chr(10)):
            s = line.strip()
            if not s:
                continue
            up = s.upper()
            if up.startswith("MEMO"):
                memo = _tag(s, 4)[:120]
                continue
            if up.startswith("LIKE"):
                for tg in _split_tags(s):
                    likes.append(tg)
                continue
            if up.startswith("HATE"):
                for tg in _split_tags(s):
                    hates.append(tg)
                continue
            if not text:
                text = s
        return {"text": text[:120], "memo": memo, "likes": likes, "hates": hates}

    async def _remember(self, memo: str, pick, detail) -> None:
        owner = ((detail or {}).get("owner") or {}).get("name") or ((pick.get("owner") or {}).get("name")) or "?"
        title = _flat((detail or {}).get("title") or pick.get("title"), 60)
        bv = pick.get("bvid") or (detail or {}).get("bvid") or ""
        content = "B站梗：" + memo + "（在 B 站刷到，来自 " + str(owner) + " 的视频《" + str(title) + "》" + str(bv) + "）"
        engine = None
        try:
            meta = self.context.get_registered_star("astrbot_plugin_livingmemory")
            inst = getattr(meta, "star_cls", None) if meta else None
            if inst is None:
                for star in self.context.get_all_stars():
                    if "livingmemory" in str(getattr(star, "name", "")):
                        inst = getattr(star, "star_cls", None)
                        break
            engine = getattr(getattr(inst, "initializer", None), "memory_engine", None)
        except Exception as exc:
            logger.warning("[bili_drift] 取 livingmemory 失败: %s" % exc)
        if engine is not None:
            try:
                doc_id = await engine.add_memory(content=content, session_id=TARGETS[0],
                                                 persona_id="小肥鱼", importance=0.62,
                                                 metadata={"source": "bili_drift", "kind": "meme",
                                                           "bvid": bv,
                                                           "url": "https://www.bilibili.com/video/" + str(bv)})
                logger.info("[bili_drift] 记住一个梗 doc_id=%s: %s" % (doc_id, memo[:40]))
                return
            except Exception as exc:
                logger.warning("[bili_drift] 写记忆失败: %s" % exc)
        try:
            MEME_PATH.parent.mkdir(parents=True, exist_ok=True)
            with MEME_PATH.open("a", encoding="utf-8") as f:
                f.write(content + chr(10))
            logger.info("[bili_drift] 梗已落备注文件: %s" % memo[:40])
        except Exception as exc2:
            logger.error("[bili_drift] 备注也失败: %s" % exc2)

    async def _send(self, text: str, target_umo=None) -> bool:
        umos = [target_umo] if target_umo else list(TARGETS)
        ok_any = False
        for umo in umos:
            sent = False
            for attempt in range(3):
                try:
                    await self.context.send_message(umo, MessageChain([Plain(text)]))
                    sent = True
                    ok_any = True
                    break
                except Exception as exc:
                    logger.warning("[bili_drift] 发送失败 %s（第 %d 次）: %r" % (umo, attempt + 1, exc))
                    await asyncio.sleep(5)
            if not sent:
                logger.error("[bili_drift] %s 三次都没发出去" % umo)
        return ok_any

    async def _pick_source(self, st: dict):
        roll = random.random()
        likes = st.get("likes") or []
        ups = st.get("recent_up") or []
        if roll < 0.6 or (not likes and not ups):
            return "home", await self._feed(FEED_SIZE)
        if roll < 0.8 and likes:
            kw = random.choice(likes)
            logger.info("[bili_drift] 这次搜自己的兴趣：%s" % kw)
            return "search:" + kw, await self._search(kw, FEED_SIZE)
        if ups:
            up = random.choice(ups)
            uid = str(up.get("uid") or "")
            if uid.isdigit():
                logger.info("[bili_drift] 这次去看看 %s 最近发了啥" % up.get("name"))
                got = await self._dynamic(uid, 10)
                if got:
                    return "up:" + str(up.get("name")), got
        return "home", await self._feed(FEED_SIZE)

    async def _drift(self, target_umo=None) -> str:
        st = self._state()
        mood = random.choice(["没啥事干", "刚吃完饭", "有点困", "心情不错", "闲着无聊"])
        src, pool = await self._pick_source(st)
        if not pool:
            return "没刷出东西"
        seen = set(st.get("seen") or [])
        fresh = [it for it in pool if (it.get("bvid") or "") not in seen]
        pool = fresh or pool
        idx = await self._choose(pool, st, mood)
        if idx <= 0:
            logger.info("[bili_drift] 刷了半天没一条想点开的，划走了（来源 %s）" % src)
            st["last"] = int(time.time())
            st["last_text"] = ""
            self._save_state(st)
            return "没点开"
        pick = pool[idx - 1]
        bv = pick.get("bvid") or ""
        detail = await self._detail(bv) if bv else None
        if random.random() < SCROLL_AWAY_CHANCE:
            logger.info("[bili_drift] 点开 %s 看了两眼觉得没劲，划走了" % bv)
            st.setdefault("scrolled", []).append(bv)
            st["last"] = int(time.time())
            self._save_state(st)
            return "划走"
        aid = (detail or {}).get("aid")
        comments = await self._comments(aid)
        danmaku = []
        pages = (detail or {}).get("pages") or []
        if pages and random.random() < 0.55:
            danmaku = await self._danmaku(pages[0].get("cid"))
        image = None
        if random.random() < SEE_COVER_CHANCE:
            pic = (detail or {}).get("pic") or pick.get("pic")
            image = await self._cover(pic)
        watch = ""
        dur = int((detail or {}).get("duration") or pick.get("duration") or 0)
        if bv and 0 < dur <= WATCH_MAX_SEC and random.random() < WATCH_CHANCE:
            logger.info("[bili_drift] 这条 %d 秒，认真看一遍" % dur)
            ctx_lines = ["标题：" + _flat((detail or {}).get("title") or pick.get("title"), 80)]
            if comments:
                ctx_lines.append("热评：" + "；".join(comments[:4]))
            if danmaku:
                ctx_lines.append("弹幕：" + " / ".join(danmaku[:10]))
            watch = await self._watch(bv, chr(10).join(ctx_lines))
        said = await self._speak(pick, detail, comments, danmaku, image, watch, st)
        text = said.get("text") or ""
        if not text:
            return "没生成出来"
        if bv and bv not in text:
            text = text + " " + bv
        await self._send(text, target_umo)
        if said.get("memo"):
            await self._remember(said["memo"], pick, detail)
        for tg in said.get("likes") or []:
            if tg not in st["likes"]:
                st["likes"].append(tg)
        for tg in said.get("hates") or []:
            if tg not in st["hates"]:
                st["hates"].append(tg)
        owner = (detail or {}).get("owner") or {}
        if owner.get("mid"):
            st["recent_up"].append({"uid": str(owner.get("mid")), "name": str(owner.get("name") or "")})
        st.setdefault("seen", []).append(bv)
        st["last"] = int(time.time())
        st["last_text"] = text
        self._save_state(st)
        logger.info("[bili_drift] 点开 %s（%s，弹幕 %d，封面 %s，看成片 %s）说：%s"
                    % (bv, src, len(danmaku), "有" if image else "无", "有" if watch else "无", text[:40]))
        return text

    def _next_gap(self, hour: float) -> int:
        base = random.randint(MIN_GAP, MAX_GAP)
        if 19 <= hour <= 23:
            base = int(base * 0.7)
        elif 9 <= hour <= 12:
            base = int(base * 1.35)
        return base

    async def _run_once(self):
        if self._busy:
            return
        self._busy = True
        try:
            await self._drift()
            if random.random() < STAY_TUNE_CHANCE:
                self._tune_next = time.time() + random.randint(90, 420)
        finally:
            self._busy = False

    async def _loop(self) -> None:
        await asyncio.sleep(60)
        next_at = time.time() + random.randint(MIN_GAP, MAX_GAP)
        while True:
            try:
                if FIRE_PATH.exists():
                    try:
                        FIRE_PATH.unlink()
                    except Exception:
                        pass
                    logger.info("[bili_drift] 手动触发，刷一次")
                    await self._run_once()
                elif self._tune_next and time.time() >= self._tune_next:
                    self._tune_next = 0.0
                    logger.info("[bili_drift] 还没刷够，再看一会儿")
                    await self._run_once()
                elif time.time() >= next_at:
                    now = datetime.utcnow() + timedelta(hours=8)
                    hour = now.hour + now.minute / 60.0
                    if START_HOUR <= hour <= END_HOUR and random.random() >= SKIP_CHANCE:
                        await self._run_once()
                    next_at = time.time() + self._next_gap(hour)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("[bili_drift] 循环异常: %s" % exc)
            await asyncio.sleep(60)

    @filter.command("刷一下")
    async def drift_now(self, event: AstrMessageEvent):
        """立刻刷一次（测试用）"""
        text = await self._drift(target_umo=event.unified_msg_origin)
        yield event.plain_result("刷完：" + str(text)[:60])
