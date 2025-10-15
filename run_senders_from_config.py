# python run_senders_from_config.py --zmq --dest-path "/mnt/bt3-disk-01/data_transfer_test/"
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


def extract_ball_id_from_frame_id(frame_id: str) -> str:
    """Extract ball_id from frame_id like 'frame_camera01_000046836.jpg' -> 'camera01'."""
    m = re.match(r"frame_([^_]+)_\d{9}\.jpg$", frame_id)
    if not m:
        raise ValueError(f"Invalid frame_id format for ball_id extraction: {frame_id}")
    return m.group(1)


def extract_camera_name_from_src_dir(src_dir: str) -> str:
    """Extract camera name from src-dir (last directory component)."""
    return os.path.basename(os.path.normpath(src_dir))


def construct_dest_path(camera_dest_path: str, ball_id: str, camera_name: str) -> str:
    """Construct destination path as camera_dest_path/ball_id/camera_name."""
    if not camera_dest_path:
        return ""
    return os.path.join(camera_dest_path, ball_id, camera_name).replace('\\', '/')


def main() -> int:
    ap = argparse.ArgumentParser(description="Launch multiple senders from camera_config.json")
    ap.add_argument("--detach", action="store_true", help="Start senders in background and exit immediately")
    ap.add_argument("--timeout-secs", type=float, default=0.0, help="For threaded (non-detach) mode: max seconds to run before stopping (0 = no timeout)")
    # PUB/SUB mode only
    ap.add_argument("--zmq-sub", action="store_true", help="Run a ZMQ SUB client to trigger senders from published payloads")
    ap.add_argument("--zmq-sub-endpoint", type=str, default="tcp://127.0.0.1:5876", help="ZMQ SUB endpoint to connect to (e.g., tcp://<host>:<port>)")
    ap.add_argument("--zmq-sub-topic", type=str, default="", help="Optional topic to subscribe to (empty subscribes to all)")
    # Forwarding (PUB) options
    ap.add_argument("--forward-pub", action="store_true", help="Enable forwarding of received triggers to other machines via PUB")
    ap.add_argument("--forward-bind", type=str, default="tcp://*:5876", help="PUB bind endpoint used to forward triggers (e.g., tcp://*:5876)")
    ap.add_argument("--forward-topic", type=str, default="", help="Optional PUB topic when forwarding (empty sends raw JSON only)")
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
    conns = 8
    lookahead = 4
    stable_ms = 1
    stable_passes = 1
    max_files = 899

    def launch_senders_with_suffix(start_after_suffix: str, ball_id: str = "") -> int:
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

            # Get destination path from config
            camera_dest_path = cfg.get("dest_path", "")
            if not camera_dest_path:
                print(f"Skipping {cam_name}: missing 'dest_path' in config", file=sys.stderr)
                continue

            # Derive start-after like frame_<camera>_<suffix>.jpg
            start_after = f"frame_{cam_name}_{start_after_suffix}.jpg"
            port = base_port + idx

            # Construct destination path
            camera_name = extract_camera_name_from_src_dir(src_dir)
            dest_path = construct_dest_path(camera_dest_path, ball_id, camera_name)
            
            print(f"[config] {camera_name}, {ball_id}")
            print(f"[config] {cam_name}: src={src_dir} -> dest={dest_path}")

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
                "--cleanup-part-files",
            ]
            
            # Add destination path (always provided from config)
            print(f"[DEST] {cam_name}: Frames will be copied to: {dest_path}/<filename>")
            print(f"[DEST] Example: {dest_path}/frame_{cam_name}_{start_after_suffix}.jpg")
            cmd.extend(["--dest-path", dest_path])
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

        # Wait for threads to finish (with optional timeout)
        if args.timeout_secs > 0:
            # Wait for up to timeout for threads to finish
            end_time = time.time() + args.timeout_secs
            for t in threads:
                remaining = end_time - time.time()
                if remaining <= 0:
                    break
                t.join(timeout=max(0.0, remaining))
            
            # If any threads are still alive after timeout, terminate their processes
            still_alive = [t for t in threads if t.is_alive()]
            if still_alive:
                print(f"[timeout] {len(still_alive)} threads still running after {args.timeout_secs}s timeout, terminating processes...", file=sys.stderr)
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
        else:
            # No timeout - wait for all threads to finish naturally
            for t in threads:
                t.join()

        if results:
            failed = [(cam, port, rc) for (cam, port), rc in results.items() if rc != 0]
            if failed:
                for cam_name, port, rc in failed:
                    print(f"Command failed for {cam_name} on port {port} with exit code {rc}", file=sys.stderr)
                return 1

        return 0

    if args.zmq_sub:
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        connect_addr = args.zmq_sub_endpoint
        socket.connect(connect_addr)
        # Topic subscription (empty string subscribes to all)
        topic = args.zmq_sub_topic.encode("utf-8") if isinstance(args.zmq_sub_topic, str) else args.zmq_sub_topic
        socket.setsockopt(zmq.SUBSCRIBE, topic)
        print(f"[zmq-sub] Subscribed to '{args.zmq_sub_topic}' on {connect_addr}")
        # Allow time for subscription to propagate
        time.sleep(0.2)
        print("[zmq-sub] Waiting for published messages (JSON with frame_id, ball_id)...")
        # Optional forwarder PUB socket
        pub_socket = None
        if args.forward_pub:
            pub_socket = context.socket(zmq.PUB)
            pub_socket.bind(args.forward_bind)
            print(f"[forward] PUB bound on {args.forward_bind} (topic='{args.forward_topic}')")
        try:
            while True:
                try:
                    # Support both single-part (raw JSON) and multipart ([topic, json]) publishers
                    parts = socket.recv_multipart()
                    if len(parts) == 1:
                        payload_bytes = parts[0]
                        topic_str = None
                    else:
                        topic_str = parts[0].decode("utf-8", errors="ignore")
                        payload_bytes = parts[-1]
                    try:
                        msg_str = payload_bytes.decode("utf-8")
                        data = json.loads(msg_str)
                    except Exception as e:
                        print(f"[zmq-sub] invalid message: {e}")
                        continue
                    print(f"[zmq-sub] received{f' topic={topic_str}' if topic_str else ''}: {data}")
                    # Ignore if capture is stopped
                    if isinstance(data, dict) and data.get("isStopped") is True:
                        print("[zmq-sub] IGNORED: isStopped True; no action taken")
                        continue
                    if not isinstance(data, dict) or "frame_id" not in data:
                        print("[zmq-sub] ERROR: missing 'frame_id'")
                        continue
                    frame_id = data.get("frame_id", "")
                    ball_id = data.get("ball_id", "default")
                    try:
                        num = parse_frame_number_from_id(frame_id)
                    except ValueError as e:
                        print(f"[zmq-sub] ERROR: {e}")
                        continue
                    suffix = f"{num:09d}"
                    print(f"[zmq-sub] launching senders start-after suffix={suffix}, ball_id={ball_id}")
                    # Forward to others if enabled
                    if pub_socket is not None:
                        try:
                            forward_payload = json.dumps(data)
                            if args.forward_topic:
                                pub_socket.send_multipart([args.forward_topic.encode("utf-8"), forward_payload.encode("utf-8")])
                            else:
                                pub_socket.send_string(forward_payload)
                            print("[forward] published trigger to subscribers")
                        except Exception as fe:
                            print(f"[forward] publish error: {fe}")
                    rc = launch_senders_with_suffix(suffix, ball_id)
                    if rc == 0:
                        print(f"[zmq-sub] SUCCESS: launched with start-after={suffix}, ball_id={ball_id}")
                    else:
                        print(f"[zmq-sub] ERROR: one or more senders failed (rc={rc})")
                except KeyboardInterrupt:
                    break
                except Exception as e:
                    print(f"[zmq-sub] error: {e}")
        finally:
            try:
                socket.close(0)
                if pub_socket is not None:
                    pub_socket.close(0)
            finally:
                context.term()
        return 0

    # Non-ZMQ path: use a default suffix and ball_id
    start_after_suffix = "000000000"
    default_ball_id = "default"  # Default ball_id when not using ZMQ
    return launch_senders_with_suffix(start_after_suffix, default_ball_id)


if __name__ == "__main__":
    raise SystemExit(main())


