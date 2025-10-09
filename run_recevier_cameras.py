#!/usr/bin/env python3
import argparse
import json
import os
import signal
import subprocess
import sys
import threading
import time
import zmq
from typing import Dict, Tuple, List, Optional


def build_out_dir(camera_name: str, cfg: Dict[str, str], ball_id: Optional[str] = None) -> str:
    dest_base = cfg.get("dest_base")
    if not dest_base:
        raise ValueError(f"camera {camera_name} missing 'dest_base' in config")
    
    if ball_id:
        return os.path.join(dest_base, ball_id, camera_name)
    else:
        return os.path.join(dest_base, camera_name)


def zmq_listener(processes: List[Tuple[str, int, subprocess.Popen]], cameras: Dict[str, Dict[str, str]], 
                base_port: int, workers: int, listen_ip: str, recv_script: str):
    """ZMQ listener that receives ball_id and updates camera processes accordingly."""
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    bind_addr = f"tcp://*:5555"
    socket.bind(bind_addr)
    
    print("ZMQ listener started, waiting for ball_id messages...")
    
    while True:
        try:
            # REQ/REP pattern: receive request and send response
            message = socket.recv_json()
            ball_id = message.get("ball_id")
            frame_id = message.get("frame_id")
            is_stopped = message.get("isStopped", False)
            diskpaths = message.get("diskpaths", [])
            
            print(f"Received ZMQ message: ball_id={ball_id}, frame_id={frame_id}, isStopped={is_stopped}")
            
            if ball_id:
                # Restart all camera processes with new ball_id
                print(f"Restarting camera processes with ball_id: {ball_id}")
                
                # Terminate existing processes
                for camera_name, port, proc in processes:
                    try:
                        proc.terminate()
                        proc.wait(timeout=2)
                    except Exception as e:
                        print(f"Error terminating {camera_name}: {e}")
                        try:
                            proc.kill()
                        except Exception:
                            pass
                
                processes.clear()
                
                # Start new processes with ball_id
                for idx, (camera_name, cfg) in enumerate(sorted(cameras.items())):
                    try:
                        out_dir = build_out_dir(camera_name, cfg, ball_id)
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
                    ]
                    print(f"Starting {camera_name} on {listen_ip}:{port} -> {out_dir}")
                    print("  ", " ".join(cmd))
                    p = subprocess.Popen(cmd)
                    processes.append((camera_name, port, p))
                
                # Send response back to client
                response = {"status": "success", "message": f"Camera processes restarted with ball_id: {ball_id}"}
                socket.send_json(response)
            else:
                # Send error response
                response = {"status": "error", "message": "No ball_id provided in message"}
                socket.send_json(response)
                    
        except Exception as e:
            print(f"Error in ZMQ listener: {e}")
            try:
                response = {"status": "error", "message": f"Server error: {str(e)}"}
                socket.send_json(response)
            except Exception:
                pass


def main() -> int:
    parser = argparse.ArgumentParser(description="Camera receiver service with optional ZMQ support")
    parser.add_argument("--zmq", action="store_true", 
                       help="Enable ZMQ service to receive ball_id messages")
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
    
    if args.zmq:
        # ZMQ mode: Start ZMQ listener and wait for messages
        zmq_thread = threading.Thread(
            target=zmq_listener, 
            args=(processes, cameras, base_port, workers, listen_ip, recv_script),
            daemon=True
        )
        zmq_thread.start()
        
        print("Camera receiver service started with ZMQ support.")
        print("Waiting for ZMQ messages to begin processing...")
        print("Send a ZMQ message with ball_id to start camera processes.")
    else:
        # Normal mode: Start cameras immediately without ZMQ
        print("Starting camera receivers in normal mode (no ZMQ)...")
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
            
            if not args.zmq and not processes:
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