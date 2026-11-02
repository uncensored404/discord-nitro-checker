#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
=====================================================================
  Discord Nitro Gift / Promo Checker   ·   by 404sec
  Modes : check (GET, sans auth) / promo (redeem, rotation tokens)
=====================================================================
  python checker.py --file codes.txt -c 10 -v --csv res.csv
  python checker.py --promo --file promos.txt --tokens tokens.txt --csv res.csv --resume
"""

import argparse
import asyncio
import csv
import json
import os
import random
import string
import sys
import time
from pathlib import Path

import aiohttp

AUTHOR   = "404sec"
BASE     = "https://discord.com/api/v9/entitlements/gift-codes"
PARAMS   = {"with_application": "false", "with_subscription_plan": "true"}
ALPHABET = string.ascii_letters + string.digits
UA       = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

CSV_HEADER = ["code", "status", "plan", "price", "uses", "max_uses",
              "expires_at", "http", "token_tail", "checked_at"]

BANNER = r"""
   ___  _  _   ___  ___  ___  ___
  | __|/ |/ | / _ \| _ \/ __|| __| 404sec
  |__ \_  _|| (_) |  _/\__ \| _|  Gift & Promo Checker
  |___/ |_|  \___/|_|  |___/|___|
"""

# ------------------------------------------------------------------ utils
def load_lines(path):
    return [l.strip() for l in Path(path).read_text(encoding="utf-8").splitlines()
            if l.strip() and not l.startswith("#")]

def norm(code):
    return (code.strip()
            .replace("https://discord.gift/", "")
            .replace("https://promos.discord.gg/", "")
            .replace("discord.gift/", "")
            .split("?")[0])

def classify(status, body):
    """Traduit une réponse API en statut lisible."""
    msg = ""
    dcode = None
    if isinstance(body, dict):
        msg = (body.get("message") or "").lower()
        dcode = body.get("code")
    if status == 200 or status == 204:
        return "REDEEMED"
    if status == 401:
        return "TOKEN_DEAD"
    if status == 429:
        return "RATELIMIT"
    if isinstance(body, dict) and body.get("captcha_key"):
        return "CAPTCHA"
    if status == 404 or dcode == 10038:
        if "already" in msg or "redeemed" in msg:
            return "ALREADY"
        if "expired" in msg:
            return "EXPIRED"
        return "INVALID"
    if "already" in msg:
        return "ALREADY"
    if "expired" in msg:
        return "EXPIRED"
    if "subscri" in msg:
        return "HAS_SUB"
    if "unknown" in msg:
        return "INVALID"
    return f"ERR{status}"

def gen_codes(n, length):
    return ["".join(random.choices(ALPHABET, k=length)) for _ in range(n)]


# ------------------------------------------------------------------ pool
class TokenPool:
    """Rotation round-robin ; les tokens morts sortent du pool."""
    def __init__(self, tokens):
        self.q = asyncio.Queue()
        self.alive = set(tokens)
        for t in tokens:
            self.q.put_nowait(t)

    async def acquire(self):
        return await self.q.get()

    def release(self, tok, dead=False):
        if dead:
            self.alive.discard(tok)
        else:
            self.q.put_nowait(tok)

    @property
    def empty(self):
        return self.q.empty() and not self.alive


# ------------------------------------------------------------------ core
class Runner:
    def __init__(self, a):
        self.a       = a
        self.lock    = asyncio.Lock()
        self.sem     = asyncio.Semaphore(a.concurrency)
        self.proxies = load_lines(a.proxies) if a.proxies else []
        self.pool    = TokenPool(load_lines(a.tokens)) if a.tokens else None
        self.stats   = dict(checked=0, valid=0, used=0, invalid=0,
                            redeemed=0, already=0, captcha=0, error=0)
        self.seen    = set()
        self.session = None
        self.csv     = None
        self.writer  = None
        if a.csv:
            new = not os.path.exists(a.csv) or os.path.getsize(a.csv) == 0
            self.csv = open(a.csv, "a", newline="", encoding="utf-8")
            self.writer = csv.writer(self.csv)
            if new:
                self.writer.writerow(CSV_HEADER)

    # ---------- infra ----------
    async def _sess(self):
        self.session = aiohttp.ClientSession(headers={
            "User-Agent": UA, "Accept": "*/*", "Accept-Language": "fr-FR,fr;q=0.9",
            "Origin": "https://discord.com", "Referer": "https://discord.com/",
        })

    def _row(self, code, status, plan="", price="", uses="", mx="",
             exp="", http="", tail=""):
        if not self.writer:
            return
        self.writer.writerow([code, status, plan, price, uses, mx, exp, http, tail,
                              time.strftime("%Y-%m-%d %H:%M:%S")])
        self.csv.flush()

    async def _webhook(self, code, plan, kind):
        if not self.a.webhook:
            return
        col = 0x57F287 if kind == "valid" else 0xFAA61A
        payload = {"embeds": [{
            "title": f"Nitro {kind} · {AUTHOR}",
            "description": f"`{code}`",
            "color": col,
            "fields": [{"name": "Plan", "value": plan or "?", "inline": True}],
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }]}
        try:
            async with self.session.post(self.a.webhook, json=payload) as r:
                await r.text()
        except Exception:
            pass

    async def _get(self, code):
        url = f"{BASE}/{code}"
        for att in range(self.a.retries):
            proxy = random.choice(self.proxies) if self.proxies else None
            try:
                async with self.session.get(url, params=PARAMS, proxy=proxy,
                                            timeout=aiohttp.ClientTimeout(total=15)) as r:
                    if r.status == 429:
                        await asyncio.sleep(float(r.headers.get("Retry-After", 2)) + random.uniform(0, .8))
                        continue
                    if r.status >= 500:
                        await asyncio.sleep(1.5 * (att + 1))
                        continue
                    txt = await r.text()
                    try:
                        return r.status, json.loads(txt)
                    except Exception:
                        return r.status, txt
            except Exception:
                await asyncio.sleep(1.0 * (att + 1))
        return 0, None

    # ---------- mode check ----------
    async def check(self, code):
        async with self.sem:
            status, data = await self._get(code)
        async with self.lock:
            self.stats["checked"] += 1
            if status == 200 and isinstance(data, dict):
                uses, mx = data.get("uses", 0), data.get("max_uses", 1)
                plan  = (data.get("subscription_plan") or {}).get("name", "")
                price = (data.get("subscription_plan") or {}).get("price", "")
                exp   = data.get("expires_at", "")
                if uses >= mx or data.get("redeemed"):
                    self.stats["used"] += 1
                    print(f"[\033[33mUSED\033[0m]    {code} ({plan})")
                    self._row(code, "USED", plan, price, uses, mx, exp, status)
                else:
                    self.stats["valid"] += 1
                    print(f"[\033[32mVALID\033[0m]   {code} -> {plan}")
                    self._row(code, "VALID", plan, price, uses, mx, exp, status)
                    with open(self.a.valid_out, "a", encoding="utf-8") as f:
                        f.write(f"{code} | {plan}\n")
                    asyncio.create_task(self._webhook(code, plan, "valid"))
                    if self.a.redeem and self.pool:
                        await self.redeem(code)
            elif status == 404:
                self.stats["invalid"] += 1
                self._row(code, "INVALID", http=status)
                if self.a.verbose:
                    print(f"[\033[90m404\033[0m]     {code}")
            else:
                self.stats["error"] += 1
                self._row(code, "ERROR", http=status)
                print(f"[\033[31mERR {status}\033[0m] {code}")
            if self.stats["checked"] % 500 == 0:
                self._stats_print()

    # ---------- mode promo / redeem ----------
    async def redeem(self, code):
        if not self.pool:
            return
        for att in range(self.a.retries):
            tok = await self.pool.acquire()
            tail = tok[-6:]
            proxy = random.choice(self.proxies) if self.proxies else None
            try:
                async with self.session.post(
                    f"{BASE}/{code}/redeem",
                    headers={"Authorization": tok, "Content-Type": "application/json"},
                    data=json.dumps({"channel_id": None, "payment_source_id": None}),
                    proxy=proxy, timeout=aiohttp.ClientTimeout(total=20)
                ) as r:
                    txt = await r.text()
                    try:
                        body = json.loads(txt)
                    except Exception:
                        body = txt
                    st = classify(r.status, body)

                    # rate limit -> on remet le token et on attend
                    if st == "RATELIMIT":
                        wait = float((body or {}).get("retry_after", 5)) + random.uniform(0, 1)
                        self.pool.release(tok)
                        await asyncio.sleep(wait)
                        continue

                    # token mort -> hors pool
                    if st == "TOKEN_DEAD":
                        self.pool.release(tok, dead=True)
                        print(f"[\033[31mTOKEN MORT\033[0m] ...{tail}")
                        continue

                    # captcha -> cooldown long sur ce token
                    if st == "CAPTCHA":
                        self.pool.release(tok)
                        async with self.lock:
                            self.stats["captcha"] += 1
                        print(f"[\033[35mCAPTCHA\033[0m] {code} (token ...{tail} en pause)")
                        await asyncio.sleep(self.a.captcha_cooldown)
                        continue

                    self.pool.release(tok)
                    await self._redeem_result(code, st, body, r.status, tail)
                    return
            except Exception:
                self.pool.release(tok)
                await asyncio.sleep(1.2 * (att + 1))
        async with self.lock:
            self.stats["error"] += 1
        print(f"[\033[31mECHEC\033[0m] {code}")

    async def _redeem_result(self, code, st, body, http, tail):
        plan = price = exp = ""
        if isinstance(body, dict):
            plan  = (body.get("subscription_plan") or {}).get("name", "")
            price = (body.get("subscription_plan") or {}).get("price", "")
            exp   = body.get("expires_at", "")
        async with self.lock:
            self.stats["checked"] += 1
            if st == "REDEEMED":
                self.stats["redeemed"] += 1
                print(f"[\033[32mREDEEMED\033[0m] {code} -> {plan} ({price})")
                self._row(code, "REDEEMED", plan, price, http=http, tail=tail)
                with open(self.a.valid_out, "a", encoding="utf-8") as f:
                    f.write(f"{code} | {plan}\n")
                asyncio.create_task(self._webhook(code, plan, "redeemed"))
                if self.a.stop_on_valid:
                    raise SystemExit(0) if False else None
            elif st == "ALREADY":
                self.stats["already"] += 1
                print(f"[\033[33mALREADY\033[0m]  {code}")
                self._row(code, "ALREADY", plan, http=http, tail=tail)
                asyncio.create_task(self._webhook(code, plan, "deja utilisé"))
            elif st in ("INVALID", "EXPIRED"):
                self.stats["invalid"] += 1
                self._row(code, st, http=http, tail=tail)
                if self.a.verbose:
                    print(f"[\033[90m{st}\033[0m] {code}")
            else:
                self.stats["error"] += 1
                self._row(code, st, http=http, tail=tail)
                print(f"[\033[31m{st}\033[0m] {code}")

    def _stats_print(self):
        s = self.stats
        print(f"\n--- {AUTHOR} | {s['checked']} traités · redeem {s['redeemed']} · "
              f"valid {s['valid']} · already {s['already']} · captcha {s['captcha']} · "
              f"err {s['error']} ---\n")

    # ---------- run ----------
    async def run(self, codes):
        await self._sess()
        t0 = time.time()

        if self.a.promo:
            q = asyncio.Queue()
            for c in codes:
                q.put_nowait(c)
            workers = min(self.a.concurrency, max(1, len(self.pool.alive)))

            async def worker():
                while not self.pool.empty:
                    try:
                        code = q.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    await self.redeem(code)
                    await asyncio.sleep(self.a.token_delay)

            await asyncio.gather(*(worker() for _ in range(workers)))
        else:
            await asyncio.gather(*(self.check(c) for c in codes), return_exceptions=True)

        await asyncio.sleep(2)
        await self.session.close()
        if self.csv:
            self.csv.close()
        self._stats_print()
        print(f"Terminé en {time.time()-t0:.1f}s")


def main():
    p = argparse.ArgumentParser(description="Discord Nitro Checker — 404sec")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--file", help="fichier de codes (liens acceptés)")
    src.add_argument("--gen", type=int, help="générer N codes aléatoires")
    p.add_argument("--len", type=int, default=16, help="longueur des codes générés")
    p.add_argument("--promo", action="store_true", help="mode promo (redeem direct, tokens requis)")
    p.add_argument("--tokens", help="fichier de tokens (1 par ligne)")
    p.add_argument("--redeem", action="store_true", help="redeem les codes valides (mode check)")
    p.add_argument("--stop-on-valid", action="store_true")
    p.add_argument("-c", "--concurrency", type=int, default=10)
    p.add_argument("-r", "--retries", type=int, default=3)
    p.add_argument("--token-delay", type=float, default=1.2, help="pause entre 2 redeem d'un même token")
    p.add_argument("--captcha-cooldown", type=float, default=25.0)
    p.add_argument("--proxies")
    p.add_argument("--webhook")
    p.add_argument("--csv", help="export CSV")
    p.add_argument("--resume", action="store_true", help="skip les codes déjà présents dans le CSV")
    p.add_argument("--out", default="valides.txt")
    p.add_argument("--valid-out", default="valides.txt")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args()

    if (a.promo or a.redeem) and not a.tokens:
        sys.exit("--promo / --redeem nécessitent --tokens")

    print(BANNER)
    codes = load_lines(a.file) if a.file else gen_codes(a.gen, a.len)
    codes = [norm(c) for c in codes]
    codes = [c for c in dict.fromkeys(codes) if c]

    if a.resume and a.csv and os.path.exists(a.csv):
        done = {row[0] for row in csv.reader(open(a.csv, encoding="utf-8")) if row}
        before = len(codes)
        codes = [c for c in codes if c not in done]
        print(f"[{AUTHOR}] resume : {before - len(codes)} codes déjà traités ignorés")

    print(f"[{AUTHOR}] {len(codes)} codes · concurrence {a.concurrency}"
          f"{' · promo' if a.promo else ''}\n")

    try:
        asyncio.run(Runner(a).run(codes))
    except KeyboardInterrupt:
        print("\nInterrompu.")


if __name__ == "__main__":
    main()