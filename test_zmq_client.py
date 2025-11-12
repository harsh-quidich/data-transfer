import zmq
import json
import time

ctx = zmq.Context()
sock = ctx.socket(zmq.PUB)
sock.bind("tcp://*:5876")  
# sock.bind("tcp://localhost:5619")       

# ---- WAIT BEFORE PUBLISHING ----
print("⏳ Waiting 2 seconds for subscribers to connect...")
time.sleep(2)  # Important!


ball_id="M5_1_0_2"

disk_paths = [
        f"/mnt/bt3-disk-01/{ball_id}/camera01",
        f"/mnt/bt3-disk-02/{ball_id}/camera03",
        f"/mnt/bt3-disk-02/{ball_id}/camera04",
        f"/mnt/bt3-disk-03/{ball_id}/camera06",
        f"/mnt/bt3-disk-03/{ball_id}/camera08",
        f"/mnt/bt3-disk-03/{ball_id}/camera09"
    ]

# model_name= "v8_s_960_ep100_july28_iter12_withaug"
# model_name= "v8_n_640_may28_iter27_batch3_fp32"
model_name = ""

ball_id = ball_id+model_name
# ball_id = ball_id
dragonfly_key = ball_id+"_v0"



# input_dirs.
payload = {
    "ball_id": ball_id,
    "frame_id": "frame_camera01_000000000.jpg",
    "dragonfly_key": dragonfly_key,
    "isStopped": False,
    "side": "FE"
}

print("Sending message....")
sock.send_json(payload)
print("📤 Published job to subscribers.")


