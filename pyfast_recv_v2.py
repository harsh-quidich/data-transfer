#!/usr/bin/env python3
import argparse, multiprocessing as mp, os, socket, struct, sys, traceback, shutil, json, time
import threading, zmq, logging
from typing import Tuple
from datetime import datetime
import redis
CHUNK = 1 << 20

REDIS_HOST, REDIS_PORT, REDIS_DB = "localhost", 6379, 0
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)

shutil.rmtree(".global_recv_state", onerror=FileNotFoundError)
# Simple file lock helpers for cross-process coordination
def with_file_lock(lock_path: str):
    fd = None
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        os.close(fd)
        return True
    except FileExistsError:
        return False
    except Exception:
        return False

def release_file_lock(lock_path: str):
    try:
        os.remove(lock_path)
    except Exception:
        pass


def recvall(sock: socket.socket, n: int) -> bytes:
    b = bytearray()
    while len(b) < n:
        chunk = sock.recv(n - len(b))
        if not chunk:
            raise ConnectionError("socket closed")
        b.extend(chunk)
    return bytes(b)

def read_u64(conn):
    b = recvall(conn, 8)
    return struct.unpack("!Q", b)[0]

MAX_NAME = 4096
MAX_DEST = 4096
MAX_DRAGONFLY_KEY = 256
MAX_SIDE = 64


def handle_client(conn, out_dir, verbose, expect_count_first, use_dest_paths, cleanup_max_count, cleanup_ttl_seconds, global_state_file, global_state_lock):
    try:
        with conn:
            peer = conn.getpeername()
            if verbose:
                logger = logging.getLogger(__name__)
                logger.info(f"[recv] connected {peer}")
            remaining_files = None
            if expect_count_first:
                remaining_files = read_u64(conn)

            did_cleanup_for_ball = False

            while True:
                try:
                    name_len = read_u64(conn)
                except ConnectionError:
                    if verbose:
                        logger = logging.getLogger(__name__)
                        logger.info(f"[recv] {peer} closed")
                    break
                if name_len > MAX_NAME:
                    raise ValueError(f"name too long: {name_len}")

                name = recvall(conn, name_len).decode()

                # Always read dest_path (sender always sends it)
                dest_len = read_u64(conn)
                if dest_len > MAX_DEST:
                    raise ValueError(f"dest too long: {dest_len}")
                dest_path = recvall(conn, dest_len).decode() if dest_len > 0 else ""

                # Read dragonfly_key
                dragonfly_key_len = read_u64(conn)
                if dragonfly_key_len > MAX_DRAGONFLY_KEY:
                    raise ValueError(f"dragonfly_key too long: {dragonfly_key_len}")
                dragonfly_key = recvall(conn, dragonfly_key_len).decode() if dragonfly_key_len > 0 else ""

                # Read side
                side_len = read_u64(conn)
                if side_len > MAX_SIDE:
                    raise ValueError(f"side too long: {side_len}")
                side = recvall(conn, side_len).decode() if side_len > 0 else ""

                logger.info(f"[recv] side dragon fly key'{dragonfly_key}'{side}")
                size = read_u64(conn)

                if use_dest_paths and not did_cleanup_for_ball and dest_path:
                    norm = os.path.normpath(dest_path)
                    parts = [p for p in norm.split(os.sep) if p and p != os.pardir and p != os.curdir]
                    if parts:
                        camera_name = os.path.basename(os.path.normpath(out_dir))
                        try:
                            cam_idx = parts.index(camera_name)
                        except ValueError:
                            cam_idx = -1

                        ball_id = parts[cam_idx - 1] if cam_idx > 0 else parts[0]

                        dest_base = os.path.dirname(out_dir)
                        sentinel_dir = os.path.join(dest_base, ".recv_sentinels", camera_name)
                        sentinel_done = os.path.join(sentinel_dir, f"{ball_id}.done")
                        sentinel_lock = sentinel_done + ".lock"

                        try:
                            os.makedirs(sentinel_dir, exist_ok=True)
                        except Exception:
                            pass

                        have_lock = False
                        try:
                            fd = os.open(sentinel_lock, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                            os.close(fd)
                            have_lock = True
                        except FileExistsError:
                            have_lock = False
                        except Exception:
                            have_lock = False

                        try:
                            state = {"count": 0, "ts": 0.0}
                            if os.path.exists(sentinel_done):
                                try:
                                    with open(sentinel_done, "r", encoding="utf-8") as f:
                                        state = json.load(f)
                                except Exception:
                                    state = {"count": 0, "ts": 0.0}

                            now = time.time()
                            if cleanup_ttl_seconds > 0 and (now - float(state.get("ts", 0.0))) >= cleanup_ttl_seconds:
                                state = {"count": 0, "ts": now}

                            should_cleanup = False
                            if cleanup_max_count > 0 and state.get("count", 0) < cleanup_max_count:
                                should_cleanup = True

                            if should_cleanup and have_lock:
                                targets = [
                                    os.path.join(out_dir, ball_id),
                                    os.path.join(dest_base, ball_id, camera_name)
                                ]
                                base_guard = os.path.normpath(dest_base)
                                for target in targets:
                                    try:
                                        target_norm = os.path.normpath(target)
                                        if not target_norm.startswith(base_guard + os.sep):
                                            continue
                                        if os.path.exists(target_norm):
                                            if verbose:
                                                logger = logging.getLogger(__name__)
                                                logger.info(f"[recv] cleanup existing data for ball_id '{ball_id}' in {target_norm}")
                                            shutil.rmtree(target_norm)
                                    except Exception:
                                        pass
                                state["count"] = int(state.get("count", 0)) + 1
                                state["ts"] = now
                                try:
                                    with open(sentinel_done, "w", encoding="utf-8") as f:
                                        json.dump(state, f)
                                except Exception:
                                    pass
                        finally:
                            if have_lock:
                                try:
                                    os.remove(sentinel_lock)
                                except Exception:
                                    pass

                        did_cleanup_for_ball = True

                if use_dest_paths:
                    final_path = os.path.join(out_dir, dest_path) if dest_path else os.path.join(out_dir, name)
                else:
                    final_path = os.path.join(out_dir, name)

                tmp_path = final_path + ".part"
                os.makedirs(os.path.dirname(final_path) or out_dir, exist_ok=True)

                remaining = size
                try:
                    with open(tmp_path, "wb", buffering=0) as f:
                        while remaining:
                            chunk = conn.recv(min(CHUNK, remaining))
                            if not chunk:
                                raise ConnectionError("socket closed mid-file")
                            f.write(chunk)
                            remaining -= len(chunk)
                        f.flush()
                        os.fsync(f.fileno())
                    os.replace(tmp_path, final_path)
                except Exception:
                    try:
                        if os.path.exists(tmp_path):
                            os.remove(tmp_path)
                    finally:
                        raise

                conn.sendall(b'\x00')

                if verbose:
                    logger = logging.getLogger(__name__)
                    target = dest_path or name
                    logger.info(f"[recv] ok  {target}")

                if use_dest_paths and dest_path:
                    norm = os.path.normpath(dest_path)
                    parts = [p for p in norm.split(os.sep) if p]
                    if len(parts) >= 3:
                        ball_id = parts[-3]
                        for _ in range(5):
                            if with_file_lock(global_state_lock):
                                try:
                                    try:
                                        with open(global_state_file, "r", encoding="utf-8") as f:
                                            gstate = json.load(f)
                                    except Exception:
                                        gstate = {}
                                    b = gstate.get(ball_id, {"count": 0, "first_emit_ts": 0.0, "last_emit_ts": 0.0})
                                    b["count"] = int(b.get("count", 0)) + 1
                                    # Store the received dragonfly_key and side
                                    if dragonfly_key:
                                        b["dragonfly_key"] = dragonfly_key
                                    if side:
                                        b["side"] = side
                                    gstate[ball_id] = b
                                    tmp_path = global_state_file + ".part"
                                    with open(tmp_path, "w", encoding="utf-8") as f:
                                        json.dump(gstate, f)
                                    os.replace(tmp_path, global_state_file)
                                finally:
                                    release_file_lock(global_state_lock)
                                break
                            else:
                                time.sleep(0.002)

                if remaining_files is not None:
                    remaining_files -= 1
                    if remaining_files == 0:
                        if verbose:
                            logger = logging.getLogger(__name__)
                            logger.info(f"[recv] received declared file count; closing {peer}")
                        break
    except Exception:
        pass


def worker(bind: Tuple[str, int], out_dir: str, reuseport: bool, verbose: bool, expect_count_first: bool,
           use_dest_paths: bool, cleanup_max_count: int, cleanup_ttl_seconds: int, global_state_file: str,
           global_state_lock: str):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if reuseport and hasattr(socket, "SO_REUSEPORT"):
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    srv.bind(bind)
    srv.listen(512)
    protocol_mode = "destination paths" if use_dest_paths else "filename only"
    logger = logging.getLogger(__name__)
    logger.info(f"[recv] listening on {bind[0]}:{bind[1]} out_dir={out_dir} protocol={protocol_mode} pid={os.getpid()}")
    try:
        while True:
            conn, _ = srv.accept()
            handle_client(conn, out_dir, verbose, expect_count_first, use_dest_paths, cleanup_max_count,
                          cleanup_ttl_seconds, global_state_file, global_state_lock)
    except KeyboardInterrupt:
        pass
    finally:
        srv.close()


def main():
    ap = argparse.ArgumentParser(description="Receiver v2 with destination path support (one-time emit)")
    ap.add_argument("--listen-ip", default="0.0.0.0")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--out-dir", required=False, default="./")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--reuseport", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--expect-count-first", action="store_true")
    ap.add_argument("--use-dest-paths", action="store_true")
    ap.add_argument("--cleanup-max-count", type=int, default=1)
    ap.add_argument("--cleanup-ttl-seconds", type=int, default=10)
    ap.add_argument("--emit-threshold", type=int, default=100)
    ap.add_argument("--zmq-endpoint", default="tcp://localhost:5623")
    ap.add_argument("--zmq-topic", default="")
    args = ap.parse_args()

    # Setup logging
    base_dir = os.path.dirname(os.path.abspath(__file__))
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"recv_{timestamp}.log"
    log_path = os.path.join(base_dir, log_filename)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logger = logging.getLogger(__name__)
    logger.info(f"Logging to file: {log_filename}")

    os.makedirs(args.out_dir, exist_ok=True)
    global_dir = os.path.join(base_dir, ".global_recv_state")
    os.makedirs(global_dir, exist_ok=True)
    global_state_file = os.path.join(global_dir, "state.json")
    global_state_lock = os.path.join(global_dir, "state.lock")
    leader_lock_file = os.path.join(global_dir, "leader.lock")

    stop_event = threading.Event()

    def acquire_leader() -> bool:
        try:
            fd = os.open(leader_lock_file, os.O_CREAT | os.O_EXCL | os.O_RDWR)
            os.close(fd)
            return True
        except FileExistsError:
            return False
        except Exception:
            return False

    def release_leader():
        try:
            os.remove(leader_lock_file)
        except Exception:
            pass

    def build_disk_paths(ball_id: str) -> list:
        try:
            cfg_path = os.path.join(base_dir, "camera_config.json")
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f)
        except Exception:
            cfg = {}
        paths = []
        for cam_name, cam_cfg in sorted(cfg.items()):
            dest_base = cam_cfg.get("dest_path")
            if not dest_base:
                continue
            dest_base = dest_base[:-1] if dest_base.endswith(os.sep) else dest_base
            disk_root = os.path.dirname(dest_base)
            paths.append(os.path.join(dest_base, ball_id, cam_name))
        return paths

    def aggregator_loop():
        ctx = zmq.Context.instance()
        pub = ctx.socket(zmq.PUB)
        pub.bind(args.zmq_endpoint)
        # print("⏳ Waiting 1 seconds for subscribers to connect...")
        # time.sleep(1)
        ready_at = time.time() + 0.2
        while not stop_event.is_set():
            if time.time() < ready_at:
                time.sleep(0.05)
                continue

            to_publish = []
            updated = False

            if with_file_lock(global_state_lock):
                try:
                    try:
                        with open(global_state_file, "r", encoding="utf-8") as f:
                            gstate = json.load(f)
                    except Exception:
                        gstate = {}

                    for ball_id, state in list(gstate.items()):
                        count = int(state.get("count", 0))
                        emitted = bool(state.get("emitted", False))
                        if not emitted and count >= args.emit_threshold:
                            now = time.time()
                            # Use the received dragonfly_key and side, with fallbacks
                            stored_dragonfly_key = state.get("dragonfly_key", f"{ball_id}_V0")
                            stored_side = state.get("side", "FE")
                            disk_paths = build_disk_paths(ball_id)
                            framepaths_key = f"{stored_dragonfly_key.replace('_V0', '')}_FRAMEPATHS"
                            diskpaths_value = "\n".join(disk_paths)

                            try:
                                redis_client.set(framepaths_key, diskpaths_value)
                                print(f"💾 Stored frame paths under '{framepaths_key}'")
                            except redis.RedisError as exc:
                                print(f"⚠️ Failed to store frame paths for {ball_id}: {exc}")
                            
                            to_publish.append({
                                "ball_id": ball_id,
                                "diskpaths": disk_paths,
                                "dragonfly_key": stored_dragonfly_key,
                                "count": count,
                                "side": stored_side
                            })
                            state.update({
                                "emitted": True,
                                "first_emit_ts": state.get("first_emit_ts", now),
                                "last_emit_ts": now
                            })
                            gstate[ball_id] = state
                            updated = True

                    if updated:
                        tmp = global_state_file + ".part"
                        with open(tmp, "w", encoding="utf-8") as f:
                            json.dump(gstate, f)
                        os.replace(tmp, global_state_file)
                finally:
                    release_file_lock(global_state_lock)

            for item in to_publish:
                try:
                    body = json.dumps({k: v for k, v in item.items() if k != "count"})
                    if args.zmq_topic:
                        pub.send_multipart([args.zmq_topic.encode("utf-8"), body.encode("utf-8")])
                    else:
                        pub.send_string(body)
                    if args.verbose:
                        logger = logging.getLogger(__name__)
                        logger.info(f"[agg] published trigger for ball_id={item['ball_id']}")
                except Exception:
                    pass
            time.sleep(0.1)

    is_leader = acquire_leader()
    if is_leader:
        agg_thread = threading.Thread(target=aggregator_loop, daemon=True)
        agg_thread.start()

    procs = []
    for _ in range(max(1, args.workers)):
        p = mp.Process(target=worker,
                       args=((args.listen_ip, args.port), args.out_dir, args.reuseport, args.verbose,
                             args.expect_count_first, args.use_dest_paths, args.cleanup_max_count,
                             args.cleanup_ttl_seconds, global_state_file, global_state_lock))
        p.start()
        procs.append(p)

    try:
        for p in procs: p.join()
    except KeyboardInterrupt:
        for p in procs: p.terminate()
        for p in procs: p.join()
    finally:
        stop_event.set()
        if is_leader:
            try:
                agg_thread.join(timeout=1)
            except Exception:
                pass
            release_leader()


if __name__ == "__main__":
    main()
