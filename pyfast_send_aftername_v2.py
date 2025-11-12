#!/usr/bin/env python3
import argparse, fnmatch, os, queue, socket, struct, threading, time, sys, json, re, logging
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime
try:
    import zmq
    ZMQ_AVAILABLE = True
except ImportError:
    ZMQ_AVAILABLE = False

# inotify removed; using polling only

CHUNK = 1 << 20  # 1 MiB

# ---------- logging setup ----------

def setup_logging(log_file: Optional[str] = None):
    """Setup logging to both file and console."""
    if log_file is None:
        log_file = os.path.join("logs", f"sender_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    
    # Create logs directory if it doesn't exist
    log_dir = os.path.dirname(log_file) if os.path.dirname(log_file) else "."
    if log_dir and not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file, mode='a'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return log_file

# ---------- filename helpers ----------

_digit_re = re.compile(r'(.*?)(\d+)(\.[^.]+)?$')

# Specific patterns to capture frame index when it's not the final numeric group
# 1) frame_camera09_000000000.jpg  -> capture 000000000
_pat_frame_after_camera = re.compile(r'^(.*?_camera\d+_)(\d+)(\.[^.]+)$')
# 2) frame_000000_camera01.jpg -> capture 000000
_pat_frame_before_camera = re.compile(r'^(frame_)(\d+)((_camera\d+)(\.[^.]+))$')

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

def split_frame_number(name: str) -> Optional[Tuple[str, int, int, str]]:
    """
    Extract the frame number and allow reconstruction with:
      prefix + zero_padded(num, width) + suffix

    Supports:
      - 'frame_camera09_000000000.jpg'  -> prefix='frame_camera09_', num=..., suffix='.jpg'
      - 'frame_000000_camera01.jpg'     -> prefix='frame_', num=..., suffix='_camera01.jpg'
    Falls back to trailing numeric group if above patterns do not match.
    """
    m = _pat_frame_after_camera.match(name)
    if m:
        prefix, digits, ext = m.group(1), m.group(2), m.group(3)
        return prefix, int(digits), len(digits), ext

    m = _pat_frame_before_camera.match(name)
    if m:
        prefix, digits, rest = m.group(1), m.group(2), m.group(3)
        return prefix, int(digits), len(digits), rest

    # Fallback: use trailing numeric suffix before extension
    return split_numeric_suffix(name)

def make_name(prefix: str, num: int, width: int, ext: str) -> str:
    return f"{prefix}{num:0{width}d}{ext}"

def lookahead_exists(src_dir: str, name: str, k: int) -> bool:
    """
    If filename ends with a zero-padded integer, check if (number+k) file exists.
    Returns True if exists; False if not or unparsable.
    """
    parts = split_frame_number(name)
    if not parts:
        return False
    prefix, num, width, ext = parts
    next_name = make_name(prefix, num + k, width, ext)
    return os.path.exists(os.path.join(src_dir, next_name))

# ---------- completeness checks ----------

def cleanup_stale_part_files(src_dir: str, pattern: str, max_age_seconds: int = 3, verbose: bool = False) -> int:
    """
    Clean up stale .part files that are older than max_age_seconds.
    Returns the number of files cleaned up.
    """
    import glob
    
    cleaned_count = 0
    current_time = time.time()
    
    try:
        # Find all .part files in the source directory
        part_pattern = pattern + '.part'
        part_files = glob.glob(os.path.join(src_dir, part_pattern))
        
        if verbose and part_files:
            logging.debug(f"[cleanup] Found {len(part_files)} .part files to check")
        
        for part_file in part_files:
            try:
                # Check file age
                file_age = current_time - os.path.getmtime(part_file)
                if file_age > max_age_seconds:
                    if verbose:
                        logging.info(f"[cleanup] Removing stale .part file: {os.path.basename(part_file)} (age: {file_age:.1f}s)")
                    os.remove(part_file)
                    cleaned_count += 1
            except (OSError, FileNotFoundError):
                # File might have been removed by another process
                pass
                
    except Exception as e:
        logging.error(f"[cleanup] Error during cleanup: {e}")
    
    return cleaned_count

def wait_for_file_and_check_complete(path: str, wait_ms: int = 5, passes: int = 1, max_wait_seconds: int = 30, file_wait_ms: int = 10) -> bool:
    """
    Wait for file to exist, then return True if file size is stable for 'passes' consecutive checks.
    
    Args:
        path: File path to check
        wait_ms: Milliseconds between size checks
        passes: Consecutive stable checks required
        max_wait_seconds: Maximum time to wait for file completion
        file_wait_ms: Milliseconds to wait when file doesn't exist yet
    """
    start_time = time.time()
    max_wait_time = max_wait_seconds
    
    # First, wait for file to exist
    while not os.path.exists(path):
        if time.time() - start_time > max_wait_time:
            return False
        time.sleep(max(0, file_wait_ms) / 1000.0)
    
    # Now check if file is complete (size stable)
    try:
        last = os.path.getsize(path)
    except FileNotFoundError:
        return False
    
    stable = 0
    
    while stable < passes:
        # Check if we've exceeded the maximum wait time
        if time.time() - start_time > max_wait_time:
            if wait_ms > 0:  # Only warn if we were actually waiting
                logging.warning(f"[WARNING] File {os.path.basename(path)} did not stabilize within {max_wait_time}s, considering it complete anyway")
            return True
            
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

# ---------- path helpers ----------

def generate_dest_path(src_dir: str, rel_name: str, dest_path_prefix: str, preserve_structure: bool) -> str:
    """
    Generate destination path for a file.
    
    Args:
        src_dir: Source directory path
        rel_name: Relative filename
        dest_path_prefix: Destination path prefix (can be empty)
        preserve_structure: Whether to preserve directory structure
    
    Returns:
        Destination path string
    """
    if not dest_path_prefix:
        return rel_name
    
    if preserve_structure:
        # Get relative path from src_dir
        full_src_path = os.path.join(src_dir, rel_name)
        try:
            # Get the relative path from src_dir to the file
            rel_path = os.path.relpath(full_src_path, src_dir)
            return os.path.join(dest_path_prefix, rel_path).replace('\\', '/')
        except ValueError:
            # If paths are on different drives (Windows), just use filename
            return os.path.join(dest_path_prefix, rel_name).replace('\\', '/')
    else:
        # Just use the filename with the destination prefix
        return os.path.join(dest_path_prefix, rel_name).replace('\\', '/')

# ---------- network send ----------

def send_file(sock, src_path, rel_name, dest_path, dragonfly_key, side, verbose, counters):
    size = os.path.getsize(src_path)
    if verbose: logging.info(f"[send] -> {rel_name} -> {dest_path} ({size} bytes)")
    
    # Send filename
    name_b = rel_name.encode()
    sock.sendall(struct.pack("!Q", len(name_b)))
    sock.sendall(name_b)
    
    # Send destination path
    dest_b = dest_path.encode()
    sock.sendall(struct.pack("!Q", len(dest_b)))
    sock.sendall(dest_b)
    
    # Send dragonfly_key
    dragonfly_key_b = (dragonfly_key or "").encode()
    sock.sendall(struct.pack("!Q", len(dragonfly_key_b)))
    sock.sendall(dragonfly_key_b)
    
    # Send side
    side_b = (side or "").encode()
    sock.sendall(struct.pack("!Q", len(side_b)))
    sock.sendall(side_b)
    
    # Send file size
    sock.sendall(struct.pack("!Q", size))

    offset = 0
    max_retries = 3
    retry_count = 0
    chunk_size = 8 << 20  # 8 MiB chunks
    
    with open(src_path, "rb", buffering=0) as f:
        while offset < size:
            try:
                # Calculate how much to send in this chunk
                to_send = min(size - offset, chunk_size)
                
                # Try sendfile first (most efficient)
                sent = sock.sendfile(f, offset=offset, count=to_send)
                
                if offset != size:
                    raise ConnectionError(f"Incomplete transfer of {rel_name}: sent {offset}/{size} bytes")

                # Read ACK with timeout to avoid hanging forever
                prev = sock.gettimeout()
                try:
                    sock.settimeout(5.0)  # tune as needed
                    ack = sock.recv(1)
                finally:
                    sock.settimeout(prev)
                if not ack:
                    raise ConnectionError("receiver closed without ACK")
                
                if sent is None:
                    # Some platforms return None; fall back to manual send
                    if verbose: logging.debug(f"[send] sendfile returned None, falling back to manual send for {rel_name}")
                    f.seek(offset)
                    data = f.read(to_send)
                    if len(data) != to_send:
                        raise IOError(f"Failed to read expected {to_send} bytes, got {len(data)}")
                    sock.sendall(data)
                    sent = len(data)
                elif sent == 0:
                    raise ConnectionError("sendfile returned 0 bytes - connection may be closed")
                elif sent < to_send:
                    # Partial send - this is normal, continue with next chunk
                    if verbose: logging.debug(f"[send] partial send for {rel_name}: {sent}/{to_send} bytes")
                
                offset += sent
                retry_count = 0  # Reset retry count on successful send
                
            except (TimeoutError, ConnectionError, OSError) as e:
                retry_count += 1
                if retry_count > max_retries:
                    raise ConnectionError(f"Failed to send {rel_name} after {max_retries} retries: {e}")
                
                if verbose: logging.warning(f"[send] retry {retry_count}/{max_retries} for {rel_name} at offset {offset}: {e}")
                
                # Fallback: finish with sendall to avoid repeated timeouts
                f.seek(offset)
                remaining = size - offset
                while remaining > 0:
                    fallback_chunk = min(remaining, 1 << 20)  # 1 MiB chunks for fallback
                    data = f.read(fallback_chunk)
                    if not data:
                        raise IOError(f"Unexpected EOF while reading {rel_name} at offset {offset}")
                    if len(data) != fallback_chunk:
                        raise IOError(f"Short read: expected {fallback_chunk}, got {len(data)} bytes")
                    sock.sendall(data)
                    offset += len(data)
                    remaining -= len(data)
                break  # Exit the while loop after manual send

    # Verify we sent the complete file
    if offset != size:
        raise ConnectionError(f"Incomplete transfer of {rel_name}: sent {offset}/{size} bytes")

    ack = sock.recv(1)
    if not ack:
        raise ConnectionError("receiver closed without ACK")
    if verbose: logging.debug(f"[send] ok {rel_name}")
    counters["files"] += 1
    counters["bytes"] += size

def worker_thread(host: str, port: int, q: "queue.Queue[Tuple[str,str,str]]", tid: int, verbose: bool, counters: dict, error_queue: "queue.Queue[Tuple[str,str,str]]", dragonfly_key: str, side: str):
    dest = (host, port); sock = None
    max_retries = 3
    
    while True:
        item = q.get()
        if item is None: break
        src_path, rel_name, dest_path = item
        
        retry_count = 0
        success = False
        
        while retry_count <= max_retries and not success:
            try:
                if sock is None:
                    if verbose: logging.info(f"[send] T{tid} connecting {dest}")
                    sock = socket.create_connection(dest, timeout=5)  # connect timeout only
                    sock.settimeout(None)                              # make I/O blocking (avoid sendfile timeouts)
                    try:
                        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    except Exception:
                        pass
                send_file(sock, src_path, rel_name, dest_path, dragonfly_key, side, verbose, counters)
                success = True
                
            except Exception as e:
                retry_count += 1
                # logging.error(f"[send] T{tid} error (attempt {retry_count}/{max_retries + 1}): {e}")
                
                # Close the socket on any error
                try:
                    if sock: sock.close()
                except Exception: 
                    pass
                sock = None
                
                if retry_count > max_retries:
                    # Final failure - report to error queue
                    error_msg = f"Failed to send {rel_name} after {max_retries + 1} attempts: {e}"
                    try:
                        error_queue.put((src_path, rel_name, error_msg), timeout=1)
                    except queue.Full:
                        logging.warning(f"[send] T{tid} error queue full, dropping error for {rel_name}")
                    break
                else:
                    # Retry - reconnect
                    try:
                        sock = socket.create_connection(dest, timeout=5)
                        sock.settimeout(None)
                        try:
                            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                        except Exception:
                            pass
                    except Exception as conn_e:
                        # logging.error(f"[send] T{tid} reconnection failed: {conn_e}")
                        if retry_count > max_retries:
                            error_msg = f"Failed to reconnect after {max_retries + 1} attempts: {conn_e}"
                            try:
                                error_queue.put((src_path, rel_name, error_msg), timeout=1)
                            except queue.Full:
                                logging.warning(f"[send] T{tid} error queue full, dropping error for {rel_name}")
                            break
        
        q.task_done()
    
    if sock: 
        try:
            sock.close()
        except Exception:
            pass

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
    ap = argparse.ArgumentParser(description="Sender (start-after filename + tail) with lookahead-complete & multi-pass stability (polling only). Supports destination path specification.")
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
    ap.add_argument("--max-wait-seconds", type=int, default=1, help="Maximum seconds to wait for file completion before giving up (prevents infinite loops)")
    ap.add_argument("--file-wait-ms", type=int, default=10, help="Milliseconds to wait when file doesn't exist yet")
    ap.add_argument("--cleanup-part-files", action="store_true", help="Clean up stale .part files on startup and periodically during tail phase")
    ap.add_argument("--part-file-max-age", type=int, default=1, help="Maximum age in seconds for .part files before cleanup (default: 1s)")
    ap.add_argument("--cleanup-interval", type=int, default=10, help="Interval in seconds for periodic .part file cleanup during tail phase (0 = disabled)")
    ap.add_argument("--scan-ms", type=int, default=50, help="Polling interval when not using inotify")

    # stop/behavior
    ap.add_argument("--max-files", type=int, default=0, help="Stop after sending this many files (0 = unlimited)")
    ap.add_argument("--once", action="store_true", help="Send current backlog and exit (no tail)")

    # path handling
    ap.add_argument("--dest-path", default="", help="Destination path prefix for files on receiver (empty = use filename only)")
    ap.add_argument("--preserve-structure", action="store_true", help="Preserve source directory structure in destination path")
    
    # metadata
    ap.add_argument("--dragonfly-key", default="", help="Dragonfly key to send with each file")
    ap.add_argument("--side", default="", help="Side information to send with each file")

    # diagnostics
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--json-stats", action="store_true", help="Print stats in JSON on exit")
    ap.add_argument("--log-file", default=None, help="Log file path (default: logs/sender_TIMESTAMP.log)")

    args = ap.parse_args()

    # Setup logging
    log_file = setup_logging(args.log_file)
    logging.info(f"Logging to file: {log_file}")

    # inotify removed; no special handling

    # Clean up stale .part files if requested
    if args.cleanup_part_files:
        cleaned = cleanup_stale_part_files(args.src_dir, args.pattern, args.part_file_max_age, args.verbose)
        if cleaned > 0:
            logging.info(f"[cleanup] Removed {cleaned} stale .part files")
        elif args.verbose:
            logging.debug(f"[cleanup] No stale .part files found")

    if args.verbose:
        logging.info(f"[send] src={args.src_dir} host={args.host}:{args.port} conns={args.conns} pattern={args.pattern}")
        logging.info(f"[send] start-after='{args.start_after}', max_files={args.max_files or '∞'}, once={args.once}")
        logging.info(f"[send] lookahead={args.lookahead}, stable_ms={args.stable_ms}, stable_passes={args.stable_passes}")
        logging.info(f"[send] max_wait_seconds={args.max_wait_seconds}, file_wait_ms={args.file_wait_ms}, cleanup_part_files={args.cleanup_part_files}, cleanup_interval={args.cleanup_interval}s")
        if args.dest_path:
            logging.info(f"[send] dest_path='{args.dest_path}', preserve_structure={args.preserve_structure}")

    q: "queue.Queue[Tuple[str,str,str]]" = queue.Queue(maxsize=max(1024, args.conns*128))
    error_q: "queue.Queue[Tuple[str,str,str]]" = queue.Queue()

    # Shared counters (protected by GIL with simple int ops)
    counters = {"files": 0, "bytes": 0}
    threads: List[threading.Thread] = []
    # Track how many files we have ENQUEUED to ensure deterministic max-files behavior
    enqueued_files = 0
    t0 = time.time()
    for i in range(args.conns):
        t = threading.Thread(target=worker_thread, args=(args.host, args.port, q, i, args.verbose, counters, error_q, args.dragonfly_key, args.side), daemon=True)
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
                if not wait_for_file_and_check_complete(full, args.stable_ms, args.stable_passes, args.max_wait_seconds, args.file_wait_ms):
                    if args.verbose:
                        logging.warning(f"[WARNING] File {name} not ready, skipping")
                    continue
            dest_path = generate_dest_path(args.src_dir, name, args.dest_path, args.preserve_structure)
            q.put((full, name, dest_path))
            enqueued_files += 1
            last_name = name


    # If only backlog is needed, drain and exit
    if args.once or (args.max_files and enqueued_files >= args.max_files):
        q.join()
    else:
        # Tail phase
        if args.verbose: logging.info("[send] entering tail phase")
        # Polling tail
        last_cleanup_time = time.time()
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
                        if not wait_for_file_and_check_complete(full, args.stable_ms, args.stable_passes, args.max_wait_seconds, args.file_wait_ms):
                            if args.verbose:
                                logging.warning(f"[WARNING] File {name} not ready, skipping")
                            continue
                    if not args.max_files or enqueued_files < args.max_files:
                        dest_path = generate_dest_path(args.src_dir, name, args.dest_path, args.preserve_structure)
                        q.put((full, name, dest_path))
                        enqueued_files += 1
                    last_name = name
            
            # Periodic cleanup of .part files during tail phase
            if args.cleanup_part_files and args.cleanup_interval > 0:
                current_time = time.time()
                if current_time - last_cleanup_time >= args.cleanup_interval:
                    cleaned = cleanup_stale_part_files(args.src_dir, args.pattern, args.part_file_max_age, args.verbose)
                    if cleaned > 0 and args.verbose:
                        logging.info(f"[cleanup] Periodic cleanup removed {cleaned} stale .part files")
                    last_cleanup_time = current_time
            
            time.sleep(max(0, args.scan_ms) / 1000.0)
        q.join()

    # Stop workers
    for _ in threads: q.put(None)
    for t in threads: t.join()

    # Check for errors
    errors = []
    while not error_q.empty():
        try:
            src_path, rel_name, error_msg = error_q.get_nowait()
            errors.append((rel_name, error_msg))
        except queue.Empty:
            break

    # Stats
    t1 = time.time()
    elapsed = max(1e-9, t1 - t0)
    mb = counters["bytes"] / (1024*1024)
    mbps = mb / elapsed
    fps = counters["files"] / elapsed

    # Report errors
    if errors:
        logging.error(f"[ERROR] {len(errors)} files failed to transfer:")
        # for rel_name, error_msg in errors:
        #     logging.error(f"[ERROR] {rel_name}: {error_msg}")
        if args.json_stats:
            print(json.dumps({"files": counters["files"], "bytes": counters["bytes"], "elapsed_s": elapsed, "MiB": mb, "MiB_per_s": mbps, "files_per_s": fps, "errors": len(errors), "error_files": [name for name, _ in errors]}))
        else:
            logging.info(f"[stats] elapsed={elapsed:.3f}s "
                  f"MiB={mb:.2f} rate={mbps:.2f} MiB/s  files/s={fps:.1f}")
        sys.exit(1)  # Exit with error code if any files failed
    else:
        if args.json_stats:
            print(json.dumps({"files": counters["files"], "bytes": counters["bytes"], "elapsed_s": elapsed, "MiB": mb, "MiB_per_s": mbps, "files_per_s": fps}))
        else:
            logging.info(f"[stats] elapsed={elapsed:.3f}s "
                  f"MiB={mb:.2f} rate={mbps:.2f} MiB/s  files/s={fps:.1f}")

if __name__ == "__main__":
    main()
