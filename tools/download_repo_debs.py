import argparse
import bz2
import gzip
import os
import re
import sys
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse


def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)


def fetch_bytes(url: str, timeout: float = 20.0, retries: int = 2, delay: float = 1.0, user_agent: Optional[str] = None) -> bytes:
    last_err: Optional[Exception] = None
    headers = {"User-Agent": user_agent or "RepoDebFetcher/1.0"}
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return resp.read()
        except Exception as ex:
            last_err = ex
            if attempt < retries:
                time.sleep(delay * (attempt + 1))
            else:
                raise
    raise last_err if last_err else RuntimeError("Unknown error fetching URL")


def try_fetch_packages(base_url: str, override_url: Optional[str], timeout: float, retries: int, delay: float, user_agent: Optional[str]) -> Tuple[str, bytes]:
    candidates = []
    if override_url:
        candidates.append(override_url)
    else:
        u = base_url.rstrip('/')
        candidates.extend([
            f"{u}/Packages.gz",
            f"{u}/Packages.bz2",
            f"{u}/Packages",
        ])
    last_err: Optional[Exception] = None
    for cu in candidates:
        try:
            data = fetch_bytes(cu, timeout=timeout, retries=retries, delay=delay, user_agent=user_agent)
            return cu, data
        except Exception as ex:
            last_err = ex
            continue
    raise RuntimeError(f"Failed to fetch Packages from {candidates}: {last_err}")


def maybe_decompress(pk_bytes: bytes, src_url: str) -> str:
    # Try gzip, then bzip2, else assume plain text
    if src_url.endswith('.gz'):
        return gzip.decompress(pk_bytes).decode('utf-8', errors='replace')
    if src_url.endswith('.bz2'):
        return bz2.decompress(pk_bytes).decode('utf-8', errors='replace')
    # Try auto-detect gzip header
    if pk_bytes[:2] == b'\x1f\x8b':
        return gzip.decompress(pk_bytes).decode('utf-8', errors='replace')
    return pk_bytes.decode('utf-8', errors='replace')


def parse_filenames_from_packages(text: str) -> List[str]:
    out: List[str] = []
    for line in text.splitlines():
        if line.lower().startswith('filename:'):
            val = line.split(':', 1)[1].strip()
            # Normalize: strip leading ./
            while val.startswith('./'):
                val = val[2:]
            out.append(val)
    # Deduplicate preserving order
    seen: Set[str] = set()
    uniq: List[str] = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


def parse_deb_hrefs_from_html(html: str, base_url: str) -> List[str]:
    # Very light parser for directory listings; find hrefs ending with .deb
    hrefs: List[str] = []
    for m in re.finditer(r'href\s*=\s*["\']([^"\'#?]+)["\']', html, re.IGNORECASE):
        href = m.group(1)
        if href.lower().endswith('.deb'):
            # Build absolute URL then try to convert to relative
            base = base_url if base_url.endswith('/') else base_url + '/'
            abs_url = urljoin(base, href)
            if abs_url.startswith(base):
                rel = abs_url[len(base):]
            else:
                rel = abs_url
            hrefs.append(rel)
    # Deduplicate
    seen: Set[str] = set()
    out: List[str] = []
    for h in hrefs:
        if h not in seen:
            seen.add(h)
            out.append(h)
    return out


def ensure_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)


def download_many(base_url: str, rel_paths: Iterable[str], dest_root: Path, timeout: float, retries: int, delay: float, user_agent: Optional[str], dry_run: bool, max_items: Optional[int]) -> Tuple[int, int, int]:
    ok = skip = fail = 0
    base = base_url.rstrip('/')
    for idx, rel in enumerate(rel_paths):
        if max_items is not None and idx >= max_items:
            break
        # Allow absolute URLs
        if rel.startswith('http://') or rel.startswith('https://'):
            url = rel
            up = urlparse(url)
            rel_norm = (up.netloc + up.path).lstrip('/')
        else:
            rel_norm = rel.lstrip('/')
            url = f"{base}/{rel_norm}"
        out_path = dest_root / rel_norm
        if out_path.exists():
            skip += 1
            print(f"[skip] exists: {out_path}")
            continue
        print(f"[get] {url}")
        if dry_run:
            ok += 1
            continue
        try:
            ensure_parent(out_path)
            data = fetch_bytes(url, timeout=timeout, retries=retries, delay=delay, user_agent=user_agent)
            out_path.write_bytes(data)
            ok += 1
            print(f"[ok] -> {out_path} ({len(data)} bytes)")
            if delay > 0:
                time.sleep(delay)
        except Exception as ex:
            fail += 1
            eprint(f"[fail] {url}: {ex}")
    return ok, skip, fail


def main():
    ap = argparse.ArgumentParser(description="Download .deb files from an APT repo for personal/offline use (no re-publishing)")
    ap.add_argument('--base-url', required=True, help='Base repo URL or a directory listing URL, e.g., https://apt.example.com or https://apt.example.com/debs/')
    gsrc = ap.add_mutually_exclusive_group(required=False)
    gsrc.add_argument('--packages-url', help='Full Packages(.gz/.bz2) URL to fetch instead of guessing')
    gsrc.add_argument('--packages-file', help='Local Packages file to parse (plain text, gz, or bz2 based on extension)')
    ap.add_argument('--dir-list', action='store_true', help='Treat base-url as a directory listing and download all .deb links found there (no Packages needed)')
    ap.add_argument('--output', default='downloads', help='Destination root folder (default: downloads)')
    ap.add_argument('--user-agent', help='Custom User-Agent header')
    ap.add_argument('--timeout', type=float, default=20.0, help='HTTP timeout seconds (default 20)')
    ap.add_argument('--retries', type=int, default=2, help='Retry attempts per request (default 2)')
    ap.add_argument('--delay', type=float, default=0.5, help='Delay seconds between downloads (default 0.5)')
    ap.add_argument('--max', type=int, help='Download at most N files')
    ap.add_argument('--dry-run', action='store_true', help='Only list actions without downloading')
    args = ap.parse_args()

    # Directory listing mode
    if args.dir_list:
        html = fetch_bytes(args.base_url, timeout=args.timeout, retries=args.retries, delay=args.delay, user_agent=args.user_agent).decode('utf-8', errors='replace')
        rel_paths = parse_deb_hrefs_from_html(html, args.base_url)
        if not rel_paths:
            raise SystemExit("No .deb links found in directory listing.")
        print(f"Found {len(rel_paths)} files to fetch from listing.")
    elif args.packages_file:
        p = Path(args.packages_file)
        if not p.exists():
            raise SystemExit(f"Packages file not found: {p}")
        data = p.read_bytes()
        src_url = p.name
        text = maybe_decompress(data, src_url)
        rel_paths = parse_filenames_from_packages(text)
        if not rel_paths:
            raise SystemExit("No Filename entries found in Packages.")
        print(f"Found {len(rel_paths)} files to fetch.")
    else:
        try:
            src_url, data = try_fetch_packages(args.base_url, args.packages_url, args.timeout, args.retries, args.delay, args.user_agent)
            text = maybe_decompress(data, src_url)
            rel_paths = parse_filenames_from_packages(text)
            if not rel_paths:
                raise SystemExit("No Filename entries found in Packages.")
            print(f"Found {len(rel_paths)} files to fetch.")
        except Exception as ex:
            print(f"Packages not found ({ex}). Trying directory listing mode...")
            html = fetch_bytes(args.base_url, timeout=args.timeout, retries=args.retries, delay=args.delay, user_agent=args.user_agent).decode('utf-8', errors='replace')
            rel_paths = parse_deb_hrefs_from_html(html, args.base_url)
            if not rel_paths:
                raise SystemExit("No .deb links found in directory listing.")
            print(f"Found {len(rel_paths)} files to fetch from listing.")

    dest_root = Path(args.output).resolve()
    ok, skip, fail = download_many(args.base_url, rel_paths, dest_root, args.timeout, args.retries, args.delay, args.user_agent, args.dry_run, args.max)
    print(f"Done. ok={ok} skip={skip} fail={fail} dest={dest_root}")


if __name__ == '__main__':
    main()
