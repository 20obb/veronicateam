import argparse
import bz2
import gzip
import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

REPO_ROOT = Path(__file__).resolve().parents[1]
DEBS = REPO_ROOT / 'debs'
PKG_FILE = REPO_ROOT / 'Packages'
ICONS_DIR_DEFAULT = REPO_ROOT / 'icons'
EOL = "\r\n"  # keep CRLF for compatibility
CHUNK = 1024 * 1024


def file_hash(path: Path, algo: str) -> str:
    h = hashlib.new(algo)
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(CHUNK), b''):
            h.update(chunk)
    return h.hexdigest()


def parse_stanzas(text: str) -> List[str]:
    parts = re.split(r"(?:\r?\n){2,}", text)
    return [p for p in parts if p.strip()]


def join_stanzas(stanzas: List[str]) -> str:
    return (EOL + EOL).join(s.strip() for s in stanzas) + EOL


def get_field(lines: List[str], key: str) -> Tuple[Optional[int], Optional[str]]:
    pattern = re.compile(rf"^\s*{re.escape(key)}\s*:\s*(.+)$", re.IGNORECASE)
    for i, ln in enumerate(lines):
        m = pattern.match(ln)
        if m:
            return i, m.group(1).strip()
    return None, None


def set_field(lines: List[str], key: str, value: str) -> List[str]:
    idx, _ = get_field(lines, key)
    line = f"{key}: {value}"
    if idx is None:
        lines.append(line)
    else:
        lines[idx] = line
    return lines


def remove_fields(lines: List[str], keys: List[str]) -> List[str]:
    keys_lc = {k.lower() for k in keys}
    out = []
    for ln in lines:
        m = re.match(r"^\s*([^:]+)\s*:\s*", ln)
        if m and m.group(1).strip().lower() in keys_lc:
            continue
        out.append(ln)
    return out


def parse_deb_filename(name: str) -> Optional[Tuple[str, str, str]]:
    # Handles names like:
    #   com.pkg.id_1.2.3-1_iphoneos-arm.deb
    #   com.pkg.id_1.2.3-1_iphoneos-arm.whatever-more.deb (extra suffix)
    if not name.endswith('.deb'):
        return None
    base = name[:-4]
    parts = base.rsplit('_', 2)
    if len(parts) != 3:
        return None
    pkg, ver, arch = parts
    if ver.startswith(('v', 'V')) and len(ver) > 1:
        ver = ver[1:]
    # Strip any extra suffix after architecture separated by '.'
    if '.' in arch:
        arch = arch.split('.', 1)[0]
    return pkg, ver, arch


@dataclass
class UpdatePlan:
    stanza_index: int
    filename: str
    size: int
    md5: str
    sha1: str
    sha256: str
    fix_pkg: Optional[str] = None
    fix_ver: Optional[str] = None
    fix_arch: Optional[str] = None
    fix_filename: Optional[str] = None
    icon_url: Optional[str] = None


def _find_icon_for_package(package_id: Optional[str], icons_dir: Path, icon_url_prefix: Optional[str]) -> Optional[str]:
    if not package_id or not icons_dir.exists():
        return None
    exts = [".png", ".jpg", ".jpeg", ".webp"]
    for ext in exts:
        candidate = icons_dir / f"{package_id}{ext}"
        if candidate.exists():
            # Build URL (absolute with prefix, else relative path under repo root)
            icon_name = candidate.name.replace('\\', '/')
            if icon_url_prefix:
                prefix = icon_url_prefix.rstrip('/')
                return f"{prefix}/{icon_name}"
            # default relative path so clients resolve against repo base
            return f"icons/{icon_name}"
    return None


def build_update_plans(stanzas: List[str], only: Optional[set], fix_metadata: bool, verbose: bool,
                       add_icons: bool = False, icons_dir: Optional[Path] = None,
                       icon_url_prefix: Optional[str] = None) -> List[UpdatePlan]:
    plans: List[UpdatePlan] = []
    for i, stanza in enumerate(stanzas):
        lines = stanza.splitlines()
        _, filename_field = get_field(lines, 'Filename')
        if not filename_field:
            continue
        deb_name = os.path.basename(filename_field)
        if only and deb_name not in only:
            continue
        deb_path = DEBS / deb_name
        actual_name = deb_name
        fix_filename = None
        if not deb_path.exists():
            # Try to find a deb with the same stem but with extra suffixes
            if deb_name.endswith('.deb'):
                stem = deb_name[:-4]
                candidates = sorted(DEBS.glob(stem + '*.deb'))
                if candidates:
                    deb_path = candidates[0]
                    actual_name = deb_path.name
                    fix_filename = f"./debs/{actual_name}"
                    if verbose:
                        print(f"[match] Resolved {deb_name} -> {actual_name}")
            if not deb_path.exists():
                if verbose:
                    print(f"[skip] missing deb: {deb_name}")
                continue
        size = deb_path.stat().st_size
        md5 = file_hash(deb_path, 'md5')
        sha1 = file_hash(deb_path, 'sha1')
        sha256 = file_hash(deb_path, 'sha256')

        fix_pkg = fix_ver = fix_arch = None
        pkg_id_val: Optional[str] = None
        if fix_metadata:
            parsed = parse_deb_filename(actual_name)
            if parsed:
                pkg, ver, arch = parsed
                _, cur_pkg = get_field(lines, 'Package')
                _, cur_ver = get_field(lines, 'Version')
                _, cur_arch = get_field(lines, 'Architecture')
                if cur_pkg != pkg:
                    fix_pkg = pkg
                if cur_ver != ver:
                    fix_ver = ver
                if cur_arch != arch:
                    fix_arch = arch
                pkg_id_val = pkg
            else:
                # fallback: read existing Package field
                _, cur_pkg = get_field(lines, 'Package')
                pkg_id_val = cur_pkg
        else:
            # No metadata fix: just read package id to match icon if needed
            _, cur_pkg = get_field(lines, 'Package')
            pkg_id_val = cur_pkg

        icon_url: Optional[str] = None
        if add_icons:
            icon_url = _find_icon_for_package(pkg_id_val, icons_dir or ICONS_DIR_DEFAULT, icon_url_prefix)

        plans.append(UpdatePlan(i, actual_name, size, md5, sha1, sha256,
                                fix_pkg, fix_ver, fix_arch, fix_filename, icon_url))
    return plans


def apply_plans(stanzas: List[str], plans: List[UpdatePlan], dry_run: bool, verbose: bool) -> List[str]:
    for plan in plans:
        lines = stanzas[plan.stanza_index].splitlines()
        # Remove old size/hash lines
        lines = remove_fields(lines, ['Size', 'MD5sum', 'SHA1', 'SHA256'])
        # Apply metadata fixes
        if plan.fix_pkg:
            lines = set_field(lines, 'Package', plan.fix_pkg)
        if plan.fix_ver:
            lines = set_field(lines, 'Version', plan.fix_ver)
        if plan.fix_arch:
            lines = set_field(lines, 'Architecture', plan.fix_arch)
        if plan.fix_filename:
            lines = set_field(lines, 'Filename', plan.fix_filename)
        if plan.icon_url:
            lines = set_field(lines, 'Icon', plan.icon_url)
        # Append updated values
        lines.append(f"Size: {plan.size}")
        lines.append(f"MD5sum: {plan.md5}")
        lines.append(f"SHA1: {plan.sha1}")
        lines.append(f"SHA256: {plan.sha256}")
        new_stanza = EOL.join(lines)
        if verbose or dry_run:
            print(
                f"[update] {plan.filename} size={plan.size} md5={plan.md5[:8]}..." +
                (f" fix: pkg={plan.fix_pkg} ver={plan.fix_ver} arch={plan.fix_arch}" if (plan.fix_pkg or plan.fix_ver or plan.fix_arch) else "") +
                (f" filename->{plan.fix_filename}" if plan.fix_filename else "") +
                (f" icon={plan.icon_url}" if plan.icon_url else "")
            )
        stanzas[plan.stanza_index] = new_stanza
    return stanzas


def write_outputs(content: str, no_compress: bool):
    # backup original
    backup = PKG_FILE.with_suffix(PKG_FILE.suffix + '.bak')
    if PKG_FILE.exists():
        backup.write_text(PKG_FILE.read_text(encoding='utf-8', newline=''), encoding='utf-8', newline='')
    PKG_FILE.write_text(content, encoding='utf-8', newline='')
    if not no_compress:
        data = content.encode('utf-8')
        with gzip.open(REPO_ROOT / 'Packages.gz', 'wb', compresslevel=9) as gzf:
            gzf.write(data)
        with bz2.open(REPO_ROOT / 'Packages.bz2', 'wb', compresslevel=9) as bzf:
            bzf.write(data)
        print(f"Wrote: Packages, Packages.gz, Packages.bz2 (backup: {backup.name})")
    else:
        print(f"Wrote: Packages (backup: {backup.name})")


def main():
    ap = argparse.ArgumentParser(description='Update APT repo Packages indices from deb files.')
    ap.add_argument('--only', nargs='*', help='Only update these deb basenames (e.g., ai.akemi.appsyncunified_116.0_iphoneos-arm.deb)')
    ap.add_argument('--fix-metadata', action='store_true', help='Align Package/Version/Architecture from deb filename if mismatched')
    ap.add_argument('--add-icons', action='store_true', help='If matching icon images exist, set Icon: for each package')
    ap.add_argument('--icons-dir', help='Directory containing per-package icon images (default: ./icons)')
    ap.add_argument('--icon-url-prefix', help='Absolute URL prefix for icon files, e.g., https://example.com/repo/icons. If omitted, a relative path icons/<file> is used.')
    ap.add_argument('--no-compress', action='store_true', help='Do not write Packages.gz / Packages.bz2')
    ap.add_argument('--dry-run', action='store_true', help='Show planned changes without writing files')
    ap.add_argument('--verbose', action='store_true', help='Verbose output')
    args = ap.parse_args()

    if not PKG_FILE.exists():
        raise SystemExit(f"Packages not found: {PKG_FILE}")
    if not DEBS.exists():
        raise SystemExit(f"debs folder not found: {DEBS}")

    raw = PKG_FILE.read_text(encoding='utf-8', newline='')
    stanzas = parse_stanzas(raw)

    only_set = set(args.only) if args.only else None
    icons_dir = Path(args.icons_dir).resolve() if args.icons_dir else ICONS_DIR_DEFAULT
    plans = build_update_plans(
        stanzas,
        only_set,
        args.fix_metadata,
        args.verbose,
        add_icons=args.add_icons,
        icons_dir=icons_dir,
        icon_url_prefix=args.icon_url_prefix,
    )

    if not plans:
        print('No stanzas matched deb files or --only selection; nothing to do.')
        return

    new_stanzas = apply_plans(stanzas, plans, args.dry_run, args.verbose)
    new_content = join_stanzas(new_stanzas)

    if args.dry_run:
        print('\n[dry-run] No files were written.')
        return

    write_outputs(new_content, args.no_compress)


if __name__ == '__main__':
    main()
