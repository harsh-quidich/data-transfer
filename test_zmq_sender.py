#!/usr/bin/env python3
"""
Test script to send ZMQ messages to the camera receiver service.
This demonstrates how to send the expected payload format.
"""
import json
import time
import zmq

def send_test_message():
    """Send a test ZMQ message with ball_id to the camera service."""
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect("tcp://localhost:5555")
    
    # Test message matching the expected format
    test_message = {
        "frame_id": "frame_camera01_000046836.jpg",
        "ball_id": "BPL_270625_1_1st_0_5",
        "isStopped": False,
        "diskpaths": []
    }
    
    print(f"Sending ZMQ message: {test_message}")
    socket.send_json(test_message)
    
    # Receive response
    response = socket.recv_json()
    print(f"Received response: {response}")
    
    # Send another test with different ball_id
    time.sleep(2)
    test_message2 = {
        "frame_id": "frame_camera02_000046837.jpg", 
        "ball_id": "BPL_270625_1_2nd_0_5",
        "isStopped": False,
        "diskpaths": []
    }
    
    print(f"Sending ZMQ message: {test_message2}")
    socket.send_json(test_message2)
    
    # Receive response
    response2 = socket.recv_json()
    print(f"Received response: {response2}")
    
    socket.close()
    context.term()
    print("Test messages sent successfully!")

if __name__ == "__main__":
    send_test_message()
