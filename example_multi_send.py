#!/usr/bin/env python3
"""
Example script showing how to use the multi-IP ZMQ client.
This script demonstrates different ways to send messages to multiple IPs.
"""

import json
import sys
import os
from multi_ip_zmq_client import MultiIPZMQClient

def example_basic_usage():
    """Basic example of sending to multiple IPs."""
    print("🔧 Example 1: Basic Multi-IP Sending")
    
    # Define target servers
    targets = [
        ("192.168.5.100", 5555),
        ("192.168.5.101", 5555),
        ("localhost", 5555)
    ]
    
    # Create client
    client = MultiIPZMQClient(targets)
    
    try:
        # Test message
        message_data = {
            "frame_id": "frame_camera01_000000001.jpg",
            "ball_id": "BPL_270625_1_1st_0_5",
            "isStopped": False,
            "diskpaths": []
        }
        
        print(f"📨 Sending message to {len(targets)} targets...")
        results = client.send_message_parallel(message_data)
        
        # Display results
        print("📊 Results:")
        for (ip, port), (success, response, error) in results.items():
            if success:
                print(f"   ✅ {ip}:{port} - {response}")
            else:
                print(f"   ❌ {ip}:{port} - {error}")
                
    finally:
        client.close()

def example_from_config():
    """Example using configuration file."""
    print("\n🔧 Example 2: Using Configuration File")
    
    config_path = "multi_targets_config.json"
    if not os.path.exists(config_path):
        print(f"❌ Config file not found: {config_path}")
        return
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Extract targets from config
    targets = [(target["ip"], target["port"]) for target in config["targets"]]
    
    # Create client
    client = MultiIPZMQClient(targets)
    
    try:
        # Use message from config
        message_data = config["default_message"]
        settings = config["settings"]
        
        print(f"📨 Sending message to {len(targets)} targets...")
        
        # Send with settings from config
        if settings["use_async"]:
            results = client.send_message_async(message_data, settings["timeout_ms"])
        else:
            results = client.send_message_parallel(message_data, settings["timeout_ms"])
        
        # Display results
        print("📊 Results:")
        for (ip, port), (success, response, error) in results.items():
            if success:
                print(f"   ✅ {ip}:{port} - {response}")
            else:
                print(f"   ❌ {ip}:{port} - {error}")
                
    finally:
        client.close()

def example_multiple_messages():
    """Example sending multiple different messages."""
    print("\n🔧 Example 3: Multiple Messages")
    
    targets = [
        ("192.168.5.101", 5555),
        ("192.168.5.102", 5555)
    ]
    
    client = MultiIPZMQClient(targets)
    
    try:
        # Different test messages
        messages = [
            {
                "frame_id": "frame_camera01_000000001.jpg",
                "ball_id": "BPL_270625_1_1st_0_5",
                "isStopped": False,
                "diskpaths": []
            },
            {
                "frame_id": "frame_camera02_000000002.jpg", 
                "ball_id": "BPL_270625_1_1st_0_6",
                "isStopped": True,
                "diskpaths": ["/ignore/me"]
            }
        ]
        
        for i, message_data in enumerate(messages, 1):
            print(f"📨 Sending message {i}/{len(messages)}...")
            results = client.send_message_parallel(message_data)
            
            print(f"📊 Results for message {i}:")
            for (ip, port), (success, response, error) in results.items():
                if success:
                    print(f"   ✅ {ip}:{port} - {response}")
                else:
                    print(f"   ❌ {ip}:{port} - {error}")
            print()
                
    finally:
        client.close()

def example_error_handling():
    """Example showing error handling with invalid targets."""
    print("\n🔧 Example 4: Error Handling")
    
    # Mix of valid and invalid targets
    targets = [
        ("192.168.5.101", 5555),  # Valid (if server running)
        ("192.168.5.999", 5555),  # Invalid IP
        ("localhost", 9999),      # Invalid port
    ]
    
    client = MultiIPZMQClient(targets)
    
    try:
        message_data = {
            "frame_id": "frame_camera01_000000001.jpg",
            "ball_id": "BPL_270625_1_1st_0_5",
            "isStopped": False,
            "diskpaths": []
        }
        
        print(f"📨 Sending message to {len(targets)} targets (some may fail)...")
        results = client.send_message_parallel(message_data, timeout=2000)  # Short timeout
        
        print("📊 Results:")
        success_count = 0
        for (ip, port), (success, response, error) in results.items():
            if success:
                print(f"   ✅ {ip}:{port} - {response}")
                success_count += 1
            else:
                print(f"   ❌ {ip}:{port} - {error}")
        
        print(f"\n📈 Summary: {success_count}/{len(targets)} targets responded successfully")
                
    finally:
        client.close()

if __name__ == "__main__":
    print("🚀 Multi-IP ZMQ Client Examples")
    print("=" * 50)
    
    try:
        example_basic_usage()
        # example_from_config()
        # example_multiple_messages()
        # example_error_handling()
        
        print("\n✅ All examples completed!")
        
    except KeyboardInterrupt:
        print("\n🛑 Examples interrupted by user")
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        sys.exit(1)
