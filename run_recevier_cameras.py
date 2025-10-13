#!/usr/bin/env python3
import argparse
import json
import os
import signal
import subprocess
import sys
from typing import Dict, Tuple, List


def build_out_dir(camera_name: str, cfg: Dict[str, str]) -> str:
    dest_base = cfg.get("dest_base")
    if not dest_base:
        raise ValueError(f"camera {camera_name} missing 'dest_base' in config")
    
    return os.path.join(dest_base, camera_name)


def main() -> int:
    parser = argparse.ArgumentParser(description="Camera receiver service")
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "camera_config.json")
    recv_script = os.path.join(base_dir, "pyfast_recv_v2.py")

    if not os.path.exists(config_path):
        print(f"Config not found: {config_path}", file=sys.stderr)
        return 1
    if not os.path.exists(recv_script):
        print(f"Receiver script not found: {recv_script}", file=sys.stderr)
        return 1

    with open(config_path, "r", encoding="utf-8") as f:
        cameras: Dict[str, Dict[str, str]] = json.load(f)

    listen_ip = "0.0.0.0"
    base_port = 50001
    workers = 16

    processes: List[Tuple[str, int, subprocess.Popen]] = []
    
    # Start cameras immediately
    print("Starting camera receivers...")
    for idx, (camera_name, cfg) in enumerate(sorted(cameras.items())):
        try:
            out_dir = build_out_dir(camera_name, cfg)
        except Exception as e:
            print(f"Skipping {camera_name}: {e}", file=sys.stderr)
            continue

        os.makedirs(out_dir, exist_ok=True)
        port = base_port + idx

        cmd = [
            sys.executable or "python3",
            recv_script,
            "--listen-ip", listen_ip,
            "--port", str(port),
            "--out-dir", out_dir,
            "--workers", str(workers),
            "--reuseport",
            "--verbose",
            "--use-dest-paths"
        ]
        print(f"Starting {camera_name} on {listen_ip}:{port} -> {out_dir}")
        print("  ", " ".join(cmd))
        p = subprocess.Popen(cmd)
        processes.append((camera_name, port, p))

    if not processes:
        print("No receiver processes started (check config)", file=sys.stderr)
        return 1

    try:
        # Keep running until we get SIGINT/SIGTERM
        while True:
            # Check if any processes have died and restart them if needed
            for camera_name, port, proc in list(processes):
                rc = proc.poll()
                if rc is not None:
                    print(f"Receiver for {camera_name} on port {port} exited with code {rc}", file=sys.stderr)
                    processes.remove((camera_name, port, proc))
            
            if not processes:
                return 1
                
            # Lightweight wait
            signal.pause()
    except KeyboardInterrupt:
        pass
    finally:
        # Clean up all processes
        for camera_name, port, proc in processes:
            try:
                proc.terminate()
            except Exception:
                pass
        for _, _, proc in processes:
            try:
                proc.wait(timeout=5)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())