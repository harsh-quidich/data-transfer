#!/usr/bin/env python3
"""
Multi-IP ZMQ client for sending messages to multiple IPs simultaneously.
Supports concurrent message sending to multiple ZMQ servers.
"""

import zmq
import time
import json
import threading
import concurrent.futures
from typing import List, Dict, Any, Tuple
import argparse
import sys


class MultiIPZMQClient:
    """ZMQ client that can send messages to multiple IPs simultaneously."""
    
    def __init__(self, targets: List[Tuple[str, int]]):
        """
        Initialize the multi-IP ZMQ client.
        
        Args:
            targets: List of (ip, port) tuples to send messages to
        """
        self.targets = targets
        self.context = zmq.Context()
        self.sockets = {}
        self._setup_sockets()
    
    def _setup_sockets(self):
        """Setup ZMQ sockets for each target."""
        for ip, port in self.targets:
            socket = self.context.socket(zmq.REQ)
            socket.connect(f"tcp://{ip}:{port}")
            self.sockets[(ip, port)] = socket
            print(f"🔗 Connected to {ip}:{port}")
    
    def send_message_single(self, message_data: Dict[str, Any], target: Tuple[str, int], timeout: int = 5000) -> Tuple[bool, str, str]:
        """
        Send a message to a single target.
        
        Args:
            message_data: JSON message data to send
            target: (ip, port) tuple of the target
            timeout: Socket timeout in milliseconds
            
        Returns:
            (success, response, error_message)
        """
        ip, port = target
        socket = self.sockets[target]
        
        try:
            # Set socket timeout
            socket.setsockopt(zmq.RCVTIMEO, timeout)
            socket.setsockopt(zmq.SNDTIMEO, timeout)
            
            # Send message
            message_json = json.dumps(message_data)
            socket.send_string(message_json)
            
            # Receive response
            response = socket.recv_string()
            return True, response, ""
            
        except zmq.Again:
            return False, "", f"Timeout sending to {ip}:{port}"
        except Exception as e:
            return False, "", f"Error sending to {ip}:{port}: {str(e)}"
    
    def send_message_parallel(self, message_data: Dict[str, Any], timeout: int = 5000) -> Dict[Tuple[str, int], Tuple[bool, str, str]]:
        """
        Send a message to all targets simultaneously using threading.
        
        Args:
            message_data: JSON message data to send
            timeout: Socket timeout in milliseconds
            
        Returns:
            Dictionary mapping (ip, port) -> (success, response, error_message)
        """
        results = {}
        
        def send_to_target(target):
            return target, self.send_message_single(message_data, target, timeout)
        
        # Use ThreadPoolExecutor for concurrent sending
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.targets)) as executor:
            # Submit all tasks
            future_to_target = {executor.submit(send_to_target, target): target for target in self.targets}
            
            # Collect results
            for future in concurrent.futures.as_completed(future_to_target):
                target, (success, response, error) = future.result()
                results[target] = (success, response, error)
        
        return results
    
    def send_message_async(self, message_data: Dict[str, Any], timeout: int = 5000) -> Dict[Tuple[str, int], Tuple[bool, str, str]]:
        """
        Send a message to all targets simultaneously using asyncio.
        
        Args:
            message_data: JSON message data to send
            timeout: Socket timeout in milliseconds
            
        Returns:
            Dictionary mapping (ip, port) -> (success, response, error_message)
        """
        import asyncio
        
        async def send_to_target_async(target):
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, self.send_message_single, message_data, target, timeout)
        
        async def send_all_async():
            tasks = [send_to_target_async(target) for target in self.targets]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            result_dict = {}
            for i, target in enumerate(self.targets):
                if isinstance(results[i], Exception):
                    result_dict[target] = (False, "", str(results[i]))
                else:
                    result_dict[target] = results[i]
            
            return result_dict
        
        return asyncio.run(send_all_async())
    
    def close(self):
        """Close all sockets and context."""
        for socket in self.sockets.values():
            socket.close()
        self.context.term()
        print("🔌 All connections closed")


def main():
    """Main function with command line interface."""
    parser = argparse.ArgumentParser(description="Multi-IP ZMQ client for sending messages to multiple servers")
    parser.add_argument("--targets", nargs="+", required=True, 
                       help="Target IPs and ports in format 'ip:port' (e.g., '192.168.1.100:5555 192.168.1.101:5555')")
    parser.add_argument("--message", type=str, 
                       default='{"frame_id": "frame_camera01_000000001.jpg", "ball_id": "BPL_270625_1_1st_0_5", "isStopped": false, "diskpaths": []}',
                       help="JSON message to send")
    parser.add_argument("--timeout", type=int, default=5000, help="Socket timeout in milliseconds")
    parser.add_argument("--async-mode", action="store_true", help="Use asyncio instead of threading")
    parser.add_argument("--repeat", type=int, default=1, help="Number of times to send the message")
    parser.add_argument("--interval", type=float, default=1.0, help="Interval between repeated sends in seconds")
    
    args = parser.parse_args()
    
    # Parse targets
    targets = []
    for target_str in args.targets:
        try:
            ip, port = target_str.split(":")
            targets.append((ip, int(port)))
        except ValueError:
            print(f"❌ Invalid target format: {target_str}. Use 'ip:port' format.")
            sys.exit(1)
    
    if not targets:
        print("❌ No valid targets provided")
        sys.exit(1)
    
    # Parse message
    try:
        message_data = json.loads(args.message)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON message: {e}")
        sys.exit(1)
    
    # Create client
    client = MultiIPZMQClient(targets)
    
    try:
        print(f"🚀 Sending message to {len(targets)} targets:")
        for ip, port in targets:
            print(f"   📍 {ip}:{port}")
        print(f"📨 Message: {json.dumps(message_data, indent=2)}")
        print()
        
        for i in range(args.repeat):
            if args.repeat > 1:
                print(f"🔄 Send #{i+1}/{args.repeat}")
            
            # Send message
            if args.async_mode:
                results = client.send_message_async(message_data, args.timeout)
            else:
                results = client.send_message_parallel(message_data, args.timeout)
            
            # Display results
            print("📊 Results:")
            for (ip, port), (success, response, error) in results.items():
                if success:
                    print(f"   ✅ {ip}:{port} - {response}")
                else:
                    print(f"   ❌ {ip}:{port} - {error}")
            
            if i < args.repeat - 1:
                print(f"⏳ Waiting {args.interval}s before next send...")
                time.sleep(args.interval)
            print()
    
    except KeyboardInterrupt:
        print("\n🛑 Interrupted by user")
    finally:
        client.close()


if __name__ == "__main__":
    main()
