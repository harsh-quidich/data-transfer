#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import argparse
import threading
import time
import re
import zmq


def parse_frame_number_from_id(frame_id: str) -> int:
    m = re.match(r"frame_[^_]+_(\d{9})\.jpg$", frame_id)
    if not m:
        raise ValueError(f"Invalid frame_id format: {frame_id}")
    return int(m.group(1))


def main() -> int:
    ap = argparse.ArgumentParser(description="Launch multiple senders from camera_config.json")
    ap.add_argument("--detach", action="store_true", help="Start senders in background and exit immediately")
    ap.add_argument("--timeout-secs", type=float, default=5.0, help="For threaded (non-detach) mode: max seconds to run before stopping")
    ap.add_argument("--zmq", action="store_true", help="Run a ZMQ REP server to trigger senders from incoming frame_id payloads")
    ap.add_argument("--zmq-port", type=int, default=5555, help="ZMQ REP port for trigger mode (avoid 5555 used elsewhere)")
    args = ap.parse_args()
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "camera_config.json")
    sender_path = os.path.join(base_dir, "pyfast_send_aftername_v2.py")

    if not os.path.exists(config_path):
        print(f"Config not found: {config_path}", file=sys.stderr)
        return 1
    if not os.path.exists(sender_path):
        print(f"Sender script not found: {sender_path}", file=sys.stderr)
        return 1

    with open(config_path, "r", encoding="utf-8") as f:
        cameras = json.load(f)

    # Fixed parameters per user example
    host = "192.168.5.101"
    base_port = 50001
    pattern = "*.jpg"
    conns = 6
    lookahead = 4
    stable_ms = 1
    stable_passes = 1
    max_files = 799

    def launch_senders_with_suffix(start_after_suffix: str) -> int:
        # Launch one sender per camera concurrently on distinct ports
        procs = []
        threads = []
        results = {}
        proc_map = {}

        def run_sender_thread(cam_name: str, port: int, cmd: list) -> None:
            p = subprocess.Popen(cmd)
            proc_map[(cam_name, port)] = p
            rc = p.wait()
            results[(cam_name, port)] = rc

        for idx, (cam_name, cfg) in enumerate(sorted(cameras.items())):
            src_dir = cfg.get("src")
            if not src_dir:
                print(f"Skipping {cam_name}: missing 'src' in config", file=sys.stderr)
                continue

            # Derive start-after like frame_<camera>_<suffix>.jpg
            start_after = f"frame_{cam_name}_{start_after_suffix}.jpg"
            port = base_port + idx

            cmd = [
                sys.executable,
                sender_path,
                "--src-dir", src_dir,
                "--start-after", start_after,
                "--host", host,
                "--port", str(port),
                "--pattern", pattern,
                "--conns", str(conns),
                "--lookahead", str(lookahead),
                "--stable-ms", str(stable_ms),
                "--stable-passes", str(stable_passes),
                "--max-files", str(max_files),
                "--once",
                "--verbose",
            ]
            print("Starting:", " ".join(cmd))
            if args.detach:
                # Start in a new session so children survive if this launcher exits
                p = subprocess.Popen(cmd, preexec_fn=os.setsid)
                procs.append((cam_name, port, p))
            else:
                t = threading.Thread(target=run_sender_thread, args=(cam_name, port, cmd), daemon=False)
                t.start()
                threads.append(t)

        if args.detach:
            for cam_name, port, p in procs:
                print(f"Started {cam_name} on port {port} with PID {p.pid}")
            return 0

        # Wait for up to timeout for threads to finish
        end_time = time.time() + max(0.0, float(args.timeout_secs))
        for t in threads:
            remaining = end_time - time.time()
            if remaining <= 0:
                break
            t.join(timeout=max(0.0, remaining))

        # If any threads are still alive after timeout, terminate their processes
        still_alive = [t for t in threads if t.is_alive()]
        if still_alive:
            for (cam_name, port), p in list(proc_map.items()):
                if p.poll() is None:
                    try:
                        p.terminate()
                    except Exception:
                        pass
            # Give a brief grace period, then kill if necessary
            grace_deadline = time.time() + 1.0
            for (cam_name, port), p in list(proc_map.items()):
                if p.poll() is None:
                    remaining = grace_deadline - time.time()
                    if remaining > 0:
                        try:
                            p.wait(timeout=remaining)
                        except Exception:
                            pass
                if p.poll() is None:
                    try:
                        p.kill()
                    except Exception:
                        pass
            # Ensure threads exit after process termination
            for t in still_alive:
                t.join(timeout=0.5)

        if results:
            failed = [(cam, port, rc) for (cam, port), rc in results.items() if rc != 0]
            if failed:
                for cam_name, port, rc in failed:
                    print(f"Command failed for {cam_name} on port {port} with exit code {rc}", file=sys.stderr)
                return 1

        return 0

    if args.zmq:
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        bind_addr = f"tcp://*:{args.zmq_port}"
        socket.bind(bind_addr)
        print(f"[zmq] Listening for triggers on {bind_addr}")
        print("[zmq] Expected JSON with 'frame_id' like 'frame_camera01_000046836.jpg' (other fields ignored)")
        try:
            while True:
                try:
                    msg = socket.recv_string()
                    print(f"[zmq] received: {msg}")
                    try:
                        data = json.loads(msg)
                    except json.JSONDecodeError as e:
                        socket.send_string(f"ERROR: invalid JSON: {e}")
                        continue
                    if "frame_id" not in data:
                        socket.send_string("ERROR: missing 'frame_id'")
                        continue
                    frame_id = data["frame_id"]
                    try:
                        num = parse_frame_number_from_id(frame_id)
                    except ValueError as e:
                        socket.send_string(f"ERROR: {e}")
                        continue
                    suffix = f"{num:09d}"
                    print(f"[zmq] launching senders start-after suffix={suffix}")
                    rc = launch_senders_with_suffix(suffix)
                    if rc == 0:
                        socket.send_string(f"SUCCESS: launched with start-after={suffix}")
                    else:
                        socket.send_string(f"ERROR: one or more senders failed (rc={rc})")
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    print(f"[zmq] error: {e}")
                    try:
                        socket.send_string(f"ERROR: {e}")
                    except Exception:
                        pass
        finally:
            try:
                socket.close(0)
            finally:
                context.term()
        return 0

    # Non-ZMQ path: use a default suffix
    start_after_suffix = "000000009"
    return launch_senders_with_suffix(start_after_suffix)


if __name__ == "__main__":
    raise SystemExit(main())


