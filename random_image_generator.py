#!/usr/bin/env python3
"""
generate_random_images.py

Generates N random JPEG images that are each between min_mb and max_mb in size.
Writes them into the specified output folder.

Requires: pillow, numpy
Install: pip install pillow numpy
"""

import os
import io
import sys
import math
import random
from pathlib import Path
from typing import Tuple

import numpy as np
from PIL import Image

# ---------- CONFIG ----------
camera_name = "camera03"
OUT_DIR = f"/mnt/EyeQ_disk1/ring_buffer_jpg/data_transfer_test/{camera_name}"
COUNT = 900                     # number of images to generate
MIN_MB = 1.0                    # minimum file size (MiB)
MAX_MB = 2.0                    # maximum file size (MiB)
BASE_WIDTH = 1600               # starting width for images
BASE_HEIGHT = 1200              # starting height for images
MAX_ATTEMPTS_PER_IMAGE = 40     # how many tries to adjust quality/size
RANDOM_SEED = None              # set to int for reproducible output or None
# ---------------------------

MIN_BYTES = int(MIN_MB * 1024 * 1024)
MAX_BYTES = int(MAX_MB * 1024 * 1024)

if RANDOM_SEED is not None:
    random.seed(RANDOM_SEED)
    np.random.seed(RANDOM_SEED)

def random_image_array(w: int, h: int) -> np.ndarray:
    """Return a random RGB uint8 numpy array shape (h, w, 3)."""
    # Use random noise; mixture of smooth and noisy helps variety
    # create per-channel random values
    arr = np.random.randint(0, 256, (h, w, 3), dtype=np.uint8)
    return arr

def pil_image_from_array(arr: np.ndarray) -> Image.Image:
    return Image.fromarray(arr, mode="RGB")

def save_jpeg_to_bytes(img: Image.Image, quality: int, subsampling: int = 0) -> bytes:
    """
    Save PIL image to bytes in memory as JPEG with given quality.
    subsampling: 0 = 4:4:4 (no chroma subsampling), 1 = 4:2:2, 2 = 4:2:0
    """
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=quality, optimize=True, subsampling=subsampling)
    return buf.getvalue()

def generate_one_image(path: Path, min_bytes: int, max_bytes: int, base_w: int, base_h: int) -> bool:
    """
    Try to generate one image that falls between min_bytes and max_bytes.
    Returns True on success (file written), False otherwise.
    """
    # Initial parameters
    attempts = 0

    # Start with base dimensions and a random seed for content
    scale = 1.0
    quality_low, quality_high = 30, 95  # search quality between these
    # We'll attempt to binary-search on quality, and if not enough, change scale
    last_good_bytes = None
    last_good_bytes_data = None

    # prepare a content seed to produce reproducible random pattern for this image
    content_seed = random.randint(0, 2**31 - 1)
    while attempts < MAX_ATTEMPTS_PER_IMAGE:
        attempts += 1
        w = max(10, int(base_w * scale))
        h = max(10, int(base_h * scale))
        # regenerate content for each attempt (different noise)
        np.random.seed(content_seed + attempts)  # slightly change noise each attempt
        arr = random_image_array(w, h)
        img = pil_image_from_array(arr)

        # Binary search on quality to reach the target size
        lo, hi = quality_low, quality_high
        best_bytes = None
        best_data = None

        for qs in range(0, 8):  # up to 8 iterations of binary search
            q = (lo + hi) // 2
            data = save_jpeg_to_bytes(img, quality=q)
            size = len(data)

            # Accept exact if in range
            if min_bytes <= size <= max_bytes:
                path.write_bytes(data)
                return True
            # record closest below or above
            if best_bytes is None or abs(size - (min_bytes + max_bytes)/2) < abs(best_bytes - (min_bytes + max_bytes)/2):
                best_bytes = size
                best_data = data

            # adjust search
            if size < min_bytes:
                # output too small -> increase quality and/or increase scale
                lo = min(q + 1, quality_high)
            else:
                # output too big -> decrease quality
                hi = max(q - 1, quality_low)

            if lo > hi:
                break

        # After quality search, check best result
        if best_data is not None:
            if min_bytes <= best_bytes <= max_bytes:
                path.write_bytes(best_data)
                return True
            # If best_bytes < min_bytes -> need bigger image: increase scale
            if best_bytes < min_bytes:
                # increase scale (but not huge)
                scale *= 1.15 + random.uniform(0.0, 0.1)
            else:
                # best_bytes > max_bytes -> reduce scale if possible
                scale *= 0.9 - random.uniform(0.0, 0.05)

        # Clamp scale to reasonable bounds
        if scale < 0.2:
            scale = 0.2
        if scale > 3.5:
            scale = 3.5

    # if we exit loop, we failed to produce valid size within attempts
    # but we can write the closest attempt (best_data) if exists to avoid skipping
    if last_good_bytes_data is not None:
        try:
            path.write_bytes(last_good_bytes_data)
            return True
        except Exception:
            return False
    return False

def ensure_out_dir(out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)

def main():
    out_dir = Path(OUT_DIR)
    ensure_out_dir(out_dir)
    print(f"Generating {COUNT} images into '{out_dir.resolve()}' each between {MIN_MB} MiB and {MAX_MB} MiB ...")
    successes = 0
    failures = 0

    for i in range(1, COUNT + 1):
        fname = f"frame_{camera_name}_{i:09d}.jpg"
        path = out_dir / fname
        ok = generate_one_image(path, MIN_BYTES, MAX_BYTES, BASE_WIDTH, BASE_HEIGHT)
        if ok:
            size = path.stat().st_size
            print(f"[{i:03d}/{COUNT}] Wrote {fname} — {size / (1024*1024):.3f} MiB")
            successes += 1
        else:
            print(f"[{i:03d}/{COUNT}] FAILED to generate {fname}")
            failures += 1

    print(f"Done. successes={successes}, failures={failures}")

if __name__ == "__main__":
    main()
