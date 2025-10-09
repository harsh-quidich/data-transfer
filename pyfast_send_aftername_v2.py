#!/usr/bin/env python3
import argparse, fnmatch, os, queue, socket, struct, threading, time, sys, json, re
from typing import List, Tuple, Optional

# inotify removed; using polling only

CHUNK = 1 << 20  # 1 MiB

# ---------- filename helpers ----------

_digit_re = re.compile(r'(.*?)(\d+)(\.[^.]+)?$')

def split_numeric_suffix(name: str) -> Optional[Tuple[str, int, int, str]]:
    """
    Split 'prefix + number + suffix' from a filename.
    Returns (prefix, number_value, number_width, suffix) or None if not matched.
    Example: 'frame_0000123.jpg' -> ('frame_', 123, 7, '.jpg')
    """
    m = _digit_re.match(name)
    if not m:
        return None
    prefix, digits, ext = m.group(1), m.group(2), (m.group(3) or '')
    return prefix, int(digits), len(digits), ext

def make_name(prefix: str, num: int, width: int, ext: str) -> str:
    return f"{prefix}{num:0{width}d}{ext}"

def lookahead_exists(src_dir: str, name: str, k: int) -> bool:
    """
    If filename ends with a zero-padded integer, check if (number+k) file exists.
    Returns True if exists; False if not or unparsable.
    """
    parts = split_numeric_suffix(name)
    if not parts:
        return False
    prefix, num, width, ext = parts
    next_name = make_name(prefix, num + k, width, ext)
    return os.path.exists(os.path.join(src_dir, next_name))

# ---------- completeness checks ----------

def is_complete(path: str, wait_ms: int = 5, passes: int = 1) -> bool:
    """Return True if file size is stable for 'passes' consecutive checks spaced wait_ms apart."""
    try:
        last = os.path.getsize(path)
    except FileNotFoundError:
        return False
    stable = 0
    while stable < passes:
        time.sleep(max(0, wait_ms) / 1000.0)
        try:
            now = os.path.getsize(path)
        except FileNotFoundError:
            return False
        if now == last:
            stable += 1
        else:
            stable = 0
            last = now
    return True

# ---------- network send ----------

def send_file(sock, src_path, rel_name, verbose, counters):
    size = os.path.getsize(src_path)
    if verbose: print(f"[send] -> {rel_name} ({size} bytes)")
    name_b = rel_name.encode()
    sock.sendall(struct.pack("!Q", len(name_b)))
    sock.sendall(name_b)
    sock.sendall(struct.pack("!Q", size))

    offset = 0
    with open(src_path, "rb", buffering=0) as f:
        while offset < size:
            try:
                # Send in chunks so we can make progress even with small socket buffers
                to_send = min(size - offset, 8 << 20)  # 8 MiB
                sent = sock.sendfile(f, offset=offset, count=to_send)
                if sent is None:
                    # Some platforms return None; fall back to manual send for this slice
                    f.seek(offset)
                    data = f.read(to_send)
                    sock.sendall(data)
                    sent = len(data)
                offset += sent
            except TimeoutError:
                # Fallback: finish with sendall to avoid repeated timeouts
                f.seek(offset)
                while offset < size:
                    data = f.read(1 << 20)
                    if not data:
                        break
                    sock.sendall(data)
                    offset += len(data)

    ack = sock.recv(1)
    if not ack:
        raise ConnectionError("receiver closed without ACK")
    if verbose: print(f"[send] ok {rel_name}")
    counters["files"] += 1
    counters["bytes"] += size

def worker_thread(host: str, port: int, q: "queue.Queue[Tuple[str,str]]", tid: int, verbose: bool, counters: dict):
    dest = (host, port); sock = None
    while True:
        item = q.get()
        if item is None: break
        src_path, rel_name = item
        try:
            if sock is None:
                if verbose: print(f"[send] T{tid} connecting {dest}")
                sock = socket.create_connection(dest, timeout=5)  # connect timeout only
                sock.settimeout(None)                              # make I/O blocking (avoid sendfile timeouts)
                try:
                    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                except Exception:
                    pass
            send_file(sock, src_path, rel_name, verbose, counters)
        except Exception as e:
            if verbose: print(f"[send] T{tid} error: {e}", file=sys.stderr)
            try:
                if sock: sock.close()
            except Exception: pass
            sock = socket.create_connection(dest, timeout=5)
            sock.settimeout(None)
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except Exception:
                pass
            send_file(sock, src_path, rel_name, verbose, counters)
        finally:
            q.task_done()
    if sock: sock.close()

# ---------- discovery ----------

def discover_once(src_dir: str, pattern: str) -> List[str]:
    try:
        names = [n for n in os.listdir(src_dir) if fnmatch.fnmatch(n, pattern)]
    except FileNotFoundError:
        return []
    names.sort()
    return names

# ---------- main ----------

def main():
    ap = argparse.ArgumentParser(description="Sender (start-after filename + tail) with lookahead-complete & multi-pass stability (polling only)")
    ap.add_argument("--src-dir", required=True)
    ap.add_argument("--start-after", default="", help="Send files with names > this (lexicographic). Empty means send all.")
    ap.add_argument("--host", required=True)
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--conns", type=int, default=8)
    ap.add_argument("--pattern", default="*.jpg")

    # completeness controls
    ap.add_argument("--lookahead", type=int, default=4, help="If (frame+lookahead) exists, treat current as complete (0 to disable)")
    ap.add_argument("--stable-ms", type=int, default=5, help="Milliseconds between size checks (used if lookahead not satisfied)")
    ap.add_argument("--stable-passes", type=int, default=1, help="Consecutive stable checks required (>=1)")
    ap.add_argument("--scan-ms", type=int, default=50, help="Polling interval when not using inotify")

    # stop/behavior
    ap.add_argument("--max-files", type=int, default=0, help="Stop after sending this many files (0 = unlimited)")
    ap.add_argument("--once", action="store_true", help="Send current backlog and exit (no tail)")
    ap.add_argument("--send-count-first", action="store_true", help="Send a 64-bit file-count header once per connection before files (requires single connection and once/backlog mode)")

    # diagnostics
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--json-stats", action="store_true", help="Print stats in JSON on exit")

    args = ap.parse_args()

    # inotify removed; no special handling

    if args.verbose:
        print(f"[send] src={args.src_dir} host={args.host}:{args.port} conns={args.conns} pattern={args.pattern}")
        print(f"[send] start-after='{args.start_after}', max_files={args.max_files or '∞'}, once={args.once}")
        print(f"[send] lookahead={args.lookahead}, stable_ms={args.stable_ms}, stable_passes={args.stable_passes}")

    q: "queue.Queue[Tuple[str,str]]" = queue.Queue(maxsize=max(1024, args.conns*128))

    # Shared counters (protected by GIL with simple int ops)
    counters = {"files": 0, "bytes": 0}
    threads: List[threading.Thread] = []
    # Track how many files we have ENQUEUED to ensure deterministic max-files behavior
    enqueued_files = 0
    t0 = time.time()
    for i in range(args.conns):
        t = threading.Thread(target=worker_thread, args=(args.host, args.port, q, i, args.verbose, counters), daemon=True)
        t.start()
        threads.append(t)

    # Backlog phase (respect start-after)
    last_name = args.start_after
    names_to_send = []
    for name in discover_once(args.src_dir, args.pattern):
        if args.max_files and enqueued_files >= args.max_files:
            break
        if name > last_name:
            full = os.path.join(args.src_dir, name)
            # NEW: lookahead-fast path
            fast_ok = False
            if args.lookahead > 0 and lookahead_exists(args.src_dir, name, args.lookahead):
                fast_ok = True
            if not fast_ok:
                while not (os.path.exists(full) and is_complete(full, args.stable_ms, args.stable_passes)):
                    time.sleep(max(0, args.stable_ms) / 1000.0)
            if args.send_count_first:
                names_to_send.append((full, name))
            else:
                q.put((full, name))
            enqueued_files += 1
            last_name = name

    # If sending count first, we require a single connection and backlog-only (no tail)
    if args.send_count_first:
        if args.conns != 1:
            print("[send] --send-count-first requires --conns=1", file=sys.stderr)
            # Drain workers if any started
            for _ in threads: q.put(None)
            for t in threads: t.join()
            return
        # Establish single connection and send count header then stream files sequentially
        dest = (args.host, args.port)
        if args.verbose: print(f"[send] connecting single socket for counted session {dest}")
        sock = socket.create_connection(dest, timeout=5)
        sock.settimeout(None)
        try:
            try:
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            except Exception:
                pass
            # Send 64-bit count first
            total_files = len(names_to_send)
            sock.sendall(struct.pack("!Q", total_files))
            if args.verbose: print(f"[send] announced file-count={total_files}")
            # Reuse send_file on the same socket
            for full, rel_name in names_to_send:
                send_file(sock, full, rel_name, args.verbose, counters)
        finally:
            try:
                sock.close()
            except Exception:
                pass
        # Skip tail and worker queueing in this mode
        threads = []
        q = None
        # Stats and exit
        t1 = time.time()
        elapsed = max(1e-9, t1 - t0)
        mb = counters["bytes"] / (1024*1024)
        mbps = mb / elapsed
        fps = counters["files"] / elapsed
        if args.json_stats:
            print(json.dumps({"files": counters["files"], "bytes": counters["bytes"], "elapsed_s": elapsed, "MiB": mb, "MiB_per_s": mbps, "files_per_s": fps}))
        else:
            print(f"[stats] files={counters['files']} bytes={counters['bytes']} elapsed={elapsed:.3f}s MiB={mb:.2f} rate={mbps:.2f} MiB/s  files/s={fps:.1f}")
        return

    # If only backlog is needed, drain and exit
    if args.once or (args.max_files and enqueued_files >= args.max_files):
        q.join()
    else:
        # Tail phase
        if args.verbose: print("[send] entering tail phase")
        # Polling tail
        while not args.max_files or enqueued_files < args.max_files:
            names = discover_once(args.src_dir, args.pattern)
            for name in names:
                if args.max_files and enqueued_files >= args.max_files:
                    break
                if name > last_name:
                    full = os.path.join(args.src_dir, name)
                    fast_ok = False
                    if args.lookahead > 0 and lookahead_exists(args.src_dir, name, args.lookahead):
                        fast_ok = True
                    if not fast_ok:
                        while not (os.path.exists(full) and is_complete(full, args.stable_ms, args.stable_passes)):
                            time.sleep(max(0, args.stable_ms) / 1000.0)
                    if not args.max_files or enqueued_files < args.max_files:
                        q.put((full, name))
                        enqueued_files += 1
                    last_name = name
            time.sleep(max(0, args.scan_ms) / 1000.0)
        q.join()

    # Stop workers
    for _ in threads: q.put(None)
    for t in threads: t.join()

    # Stats
    t1 = time.time()
    elapsed = max(1e-9, t1 - t0)
    mb = counters["bytes"] / (1024*1024)
    mbps = mb / elapsed
    fps = counters["files"] / elapsed

    if args.json_stats:
        print(json.dumps({"files": counters["files"], "bytes": counters["bytes"], "elapsed_s": elapsed, "MiB": mb, "MiB_per_s": mbps, "files_per_s": fps}))
    else:
        print(f"[stats] files={counters['files']} bytes={counters['bytes']} elapsed={elapsed:.3f}s "
              f"MiB={mb:.2f} rate={mbps:.2f} MiB/s  files/s={fps:.1f}")

if __name__ == "__main__":
    main()
