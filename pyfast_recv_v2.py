#!/usr/bin/env python3
import argparse, multiprocessing as mp, os, socket, struct, sys, traceback
from typing import Tuple
CHUNK = 1 << 20

def recvall(sock: socket.socket, n: int) -> bytes:
    b = bytearray()
    while len(b) < n:
        chunk = sock.recv(n - len(b))
        if not chunk:
            raise ConnectionError("socket closed")
        b.extend(chunk)
    return bytes(b)

def handle_client(conn: socket.socket, out_dir: str, verbose: bool, expect_count_first: bool, use_dest_paths: bool):
    try:
        with conn:
            peer = conn.getpeername()
            if verbose: print(f"[recv] connected {peer}")
            remaining_files = None
            if expect_count_first:
                hdr = conn.recv(8)
                if not hdr:
                    if verbose: print(f"[recv] {peer} closed before count header")
                    return
                remaining_files = struct.unpack("!Q", hdr)[0]
                if verbose: print(f"[recv] expecting {remaining_files} files from {peer}")
            while True:
                hdr = conn.recv(8)
                if not hdr:
                    if verbose: print(f"[recv] {peer} closed")
                    break
                name_len = struct.unpack("!Q", hdr)[0]
                name = recvall(conn, name_len).decode()
                
                # Read destination path length and path (if enabled)
                dest_path = ""
                if use_dest_paths:
                    dest_hdr = conn.recv(8)
                    if not dest_hdr:
                        if verbose: print(f"[recv] {peer} closed before dest path")
                        break
                    dest_len = struct.unpack("!Q", dest_hdr)[0]
                    dest_path = recvall(conn, dest_len).decode()
                
                size = struct.unpack("!Q", recvall(conn, 8))[0]
                
                # Determine final path based on protocol and destination path
                if use_dest_paths:
                    # New protocol: use destination path if provided, otherwise use filename directly in out_dir
                    if dest_path:
                        final_path = os.path.join(out_dir, dest_path)
                    else:
                        # Default to storing directly in out_dir when no dest_path is provided
                        final_path = os.path.join(out_dir, name)
                else:
                    # Old protocol: always use filename directly
                    final_path = os.path.join(out_dir, name)
                tmp_path = final_path + ".part"
                os.makedirs(os.path.dirname(final_path) or out_dir, exist_ok=True)
                if verbose: 
                    if use_dest_paths and dest_path:
                        print(f"[recv] <- {name} -> {dest_path} ({size} bytes)")
                    elif use_dest_paths and not dest_path:
                        print(f"[recv] <- {name} (default) ({size} bytes)")
                    else:
                        print(f"[recv] <- {name} ({size} bytes)")

                remaining = size
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
                conn.sendall(b'\x00')
                if verbose: 
                    if use_dest_paths and dest_path:
                        print(f"[recv] ok  {name} -> {dest_path}")
                    elif use_dest_paths and not dest_path:
                        print(f"[recv] ok  {name} (default)")
                    else:
                        print(f"[recv] ok  {name}")
                if remaining_files is not None:
                    remaining_files -= 1
                    if remaining_files == 0:
                        if verbose: print(f"[recv] received declared file count; closing {peer}")
                        break
    except Exception as e:
        print(f"[recv] client error: {e}\n{traceback.format_exc()}", file=sys.stderr)

def worker(bind: Tuple[str,int], out_dir: str, reuseport: bool, verbose: bool, expect_count_first: bool, use_dest_paths: bool):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if reuseport and hasattr(socket, "SO_REUSEPORT"):
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
    srv.bind(bind)
    srv.listen(512)
    protocol_mode = "destination paths" if use_dest_paths else "filename only"
    print(f"[recv] listening on {bind[0]}:{bind[1]} out_dir={out_dir} protocol={protocol_mode} pid={os.getpid()}")
    try:
        while True:
            conn, _ = srv.accept()
            handle_client(conn, out_dir, verbose, expect_count_first, use_dest_paths)
    except KeyboardInterrupt:
        pass
    finally:
        srv.close()

def main():
    ap = argparse.ArgumentParser(description="Receiver v2 with destination path support")
    ap.add_argument("--listen-ip", default="0.0.0.0")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--out-dir", required=False, default="./")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--reuseport", action="store_true")
    ap.add_argument("--verbose", action="store_true")
    ap.add_argument("--expect-count-first", action="store_true", help="Expect a 64-bit file-count header per connection and close after N files")
    ap.add_argument("--use-dest-paths", action="store_true", help="Enable destination path support (new protocol)")
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    procs = []
    for _ in range(max(1, args.workers)):
        p = mp.Process(target=worker, args=((args.listen_ip, args.port), args.out_dir, args.reuseport, args.verbose, args.expect_count_first, args.use_dest_paths))
        p.start()
        procs.append(p)
    try:
        for p in procs: p.join()
    except KeyboardInterrupt:
        for p in procs: p.terminate()
        for p in procs: p.join()

if __name__ == "__main__":
    main()
