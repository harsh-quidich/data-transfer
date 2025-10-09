#!/usr/bin/env python3
"""
Delete contents inside destination directory/directories.

Usage:
  python clear_destination.py /path/to/dest [--yes]
  python clear_destination.py --camera CAMERA_NAME [--config /path/config.json] [--yes]
  python clear_destination.py --all [--config /path/config.json] [--yes]

Behavior:
- Validates that the destination exists and is a directory
- Refuses to operate on dangerous paths (e.g., '/')
- Deletes files, directories, and symlinks inside the destination
- Keeps the destination directory itself intact
- When using --camera/--all, reads `camera_config.json` to derive out directories
"""

import argparse
import json
import os
import shutil
import sys
from pathlib import Path


def is_dangerous_path(path: Path) -> bool:
    """Heuristics to avoid catastrophic deletions."""
    try:
        resolved = path.resolve(strict=False)
    except Exception:
        # If resolve fails, be conservative
        return True

    # Disallow root and near-root locations
    if str(resolved) == "/":
        return True

    # Disallow if the directory has extremely shallow depth (e.g., '/home')
    # Require at least 3 parts like ['','home','user','something']
    if len(resolved.parts) < 3:
        return True

    return False


def clear_directory_contents(destination: Path) -> tuple[int, int]:
    """Delete all children of destination.

    Returns (num_deleted, num_errors).
    """
    num_deleted = 0
    num_errors = 0

    for entry in destination.iterdir():
        try:
            if entry.is_symlink() or entry.is_file():
                entry.unlink(missing_ok=True)
            elif entry.is_dir():
                shutil.rmtree(entry)
            else:
                # Fallback for unusual types
                entry.unlink(missing_ok=True)
            num_deleted += 1
        except Exception as exc:
            num_errors += 1
            print(f"Failed to delete '{entry}': {exc}", file=sys.stderr)

    return num_deleted, num_errors


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Delete all contents from a destination directory.")
    parser.add_argument("destination", nargs="?", type=Path, help="Path to the destination directory to clear")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--camera", help="Camera name to clear (reads from camera_config.json)")
    group.add_argument("--all", action="store_true", help="Clear all camera destinations from camera_config.json")
    parser.add_argument("--config", type=Path, help="Path to camera_config.json (defaults next to this script)")
    parser.add_argument("--yes", "-y", action="store_true", help="Do not prompt for confirmation")
    return parser.parse_args(argv)


def load_camera_config(config_path: Path) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def build_out_dir(camera_name: str, cfg: dict) -> Path:
    dest_base = cfg.get("dest_base")
    if not dest_base:
        raise ValueError(f"camera {camera_name} missing 'dest_base' in config")
    return Path(dest_base) / camera_name


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    base_dir = Path(__file__).resolve().parent

    # Determine mode: direct destination vs camera config based
    if args.destination is None and not args.camera and not args.all:
        print("Provide a destination path or use --camera/--all.", file=sys.stderr)
        return 2

    # Collect destinations to clear
    destinations: list[Path] = []
    if args.destination is not None:
        destinations.append(args.destination)

    if args.camera or args.all:
        config_path = args.config or (base_dir / "camera_config.json")
        if not config_path.exists():
            print(f"Config not found: {config_path}", file=sys.stderr)
            return 2
        try:
            cameras = load_camera_config(config_path)
        except Exception as exc:
            print(f"Failed to read config '{config_path}': {exc}", file=sys.stderr)
            return 2

        if args.camera:
            if args.camera not in cameras:
                print(f"Camera not found in config: {args.camera}", file=sys.stderr)
                return 2
            try:
                destinations.append(build_out_dir(args.camera, cameras[args.camera]))
            except Exception as exc:
                print(f"Invalid config for camera '{args.camera}': {exc}", file=sys.stderr)
                return 2
        else:
            for cam_name, cfg in sorted(cameras.items()):
                try:
                    destinations.append(build_out_dir(cam_name, cfg))
                except Exception as exc:
                    print(f"Skipping {cam_name}: {exc}", file=sys.stderr)

    # Deduplicate while preserving order
    seen = set()
    unique_destinations: list[Path] = []
    for d in destinations:
        d_res = d.resolve()
        if d_res not in seen:
            seen.add(d_res)
            unique_destinations.append(d)

    if not unique_destinations:
        print("No destinations to clear.", file=sys.stderr)
        return 1

    # Confirm once for all
    if not args.yes:
        listing = "\n  - ".join(str(p) for p in unique_destinations)
        reply = input(
            f"This will delete ALL contents inside the following directories:\n  - {listing}\nContinue? [y/N]: "
        ).strip().lower()
        if reply not in {"y", "yes"}:
            print("Aborted.")
            return 1

    overall_errors = 0
    for dest in unique_destinations:
        if not dest.exists():
            print(f"Destination does not exist: {dest}", file=sys.stderr)
            overall_errors += 1
            continue
        if not dest.is_dir():
            print(f"Destination is not a directory: {dest}", file=sys.stderr)
            overall_errors += 1
            continue
        if is_dangerous_path(dest):
            print(f"Refusing to operate on potentially dangerous path: {dest}", file=sys.stderr)
            overall_errors += 1
            continue

        deleted, errors = clear_directory_contents(dest)
        print(f"Cleared '{dest}': deleted items={deleted}")
        if errors:
            print(f"Items failed to delete in '{dest}': {errors}", file=sys.stderr)
            overall_errors += 1

    return 0 if overall_errors == 0 else 4


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))


