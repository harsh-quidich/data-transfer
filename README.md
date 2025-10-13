# Camera Data Transfer System

This directory contains a complete camera data transfer system with both sender and receiver components. The system is designed for high-performance file transfer and data management in a multi-camera environment, featuring advanced capabilities like file completion detection, parallel transfers, configurable routing, and robust error handling.

## System Overview

The system consists of two main components:
- **Sender Side**: Scripts for transferring camera data from source locations
- **Receiver Side**: Scripts for receiving and managing camera data at destination locations

Both sides work together to provide a complete data transfer solution with monitoring, cleanup, and orchestration capabilities.

## Scripts Overview

## Sender Side Scripts

### 1. `pyfast_send_aftername_v2.py`
**High-performance file sender with advanced completion detection**

A sophisticated file transfer client that sends files over TCP with intelligent file completion detection and parallel transfer capabilities.

#### Key Features:
- **File Completion Detection**: Uses lookahead checking and file stability verification to ensure files are complete before transfer
- **File Existence Waiting**: Waits for files to appear before checking completion, preventing race conditions
- **Parallel Transfers**: Multi-threaded sending with configurable connection count
- **Smart Polling**: Efficient directory scanning with configurable intervals
- **Partial File Cleanup**: Automatic cleanup of stale `.part` files
- **Destination Path Control**: Flexible destination path specification with structure preservation
- **Robust Error Handling**: Retry logic and comprehensive error reporting

#### Recent Improvements (v2):
- **Enhanced File Waiting**: New `--file-wait-ms` parameter allows configurable waiting when files don't exist yet
- **Simplified Codebase**: Removed complex `--send-count-first` functionality for cleaner, more maintainable code
- **Better Race Condition Handling**: Improved logic prevents skipping files that are still being written
- **Streamlined Cleanup**: Simplified `.part` file cleanup logic for better performance

#### Usage:
```bash
python pyfast_send_aftername_v2.py \
    --src-dir /path/to/source \
    --host 192.168.5.101 \
    --port 50004 \
    --start-after "frame_camera01_000000000.jpg" \
    --pattern "*.jpg" \
    --conns 8 \
    --lookahead 4 \
    --file-wait-ms 10 \
    --max-files 799 \
    --dest-path "/mnt/destination/" \
    --preserve-structure \
    --verbose
```

#### Key Parameters:
- `--src-dir`: Source directory to monitor
- `--start-after`: Only send files with names lexicographically after this
- `--host/--port`: Destination server address
- `--conns`: Number of parallel connections (default: 8)
- `--lookahead`: Check if file+N exists to determine completion (default: 4)
- `--stable-ms`: Milliseconds between size stability checks (default: 5)
- `--file-wait-ms`: Milliseconds to wait when file doesn't exist yet (default: 10)
- `--max-files`: Maximum files to send (0 = unlimited)
- `--dest-path`: Destination path prefix
- `--preserve-structure`: Maintain directory structure in destination
- `--cleanup-part-files`: Remove stale .part files
- `--once`: Send current backlog and exit (no continuous monitoring)

### 2. `random_image_generator.py`
**Test image generator for camera simulation**

Generates random JPEG images of specified sizes for testing the transfer system. Creates images for multiple camera sources with realistic file sizes and naming conventions.

#### Features:
- **Configurable Image Sizes**: Generate images between specified min/max file sizes
- **Multiple Camera Support**: Creates images for multiple camera directories
- **Realistic Naming**: Uses frame naming convention `frame_{camera}_{number:09d}.jpg`
- **Quality Optimization**: Binary search on JPEG quality to achieve target file sizes
- **Reproducible Output**: Optional random seed for consistent test data

#### Configuration (at top of file):
```python
camera_names = ["camera01", "camera02", "camera03", "camera04", "camera05", "camera06"]
OUT_DIR = "/mnt/EyeQ_disk1/ring_buffer_jpg/data_transfer_test/"
COUNT = 900                     # number of images per camera
MIN_MB = 1.0                    # minimum file size (MiB)
MAX_MB = 2.0                    # maximum file size (MiB)
BASE_WIDTH = 1600               # starting image width
BASE_HEIGHT = 1200              # starting image height
```

#### Usage:
```bash
python random_image_generator.py
```

#### Dependencies:
```bash
pip install pillow numpy
```

### 3. `run_senders_from_config.py`
**Multi-camera sender launcher with ZMQ trigger support**

Orchestrates multiple file senders based on camera configuration. Supports both direct execution and ZMQ-triggered operation for integration with external systems.

#### Features:
- **Configuration-Driven**: Uses `camera_config.json` to define camera sources and destinations
- **Parallel Launch**: Starts multiple senders concurrently on different ports
- **ZMQ Integration**: Optional ZMQ REP server for external triggering
- **Ball ID Support**: Dynamic destination path construction based on ball/camera IDs
- **Timeout Management**: Configurable timeouts for sender processes
- **Detached Mode**: Option to run senders in background

#### Usage:

**Direct execution:**
```bash
python run_senders_from_config.py
```

**ZMQ trigger mode:**
We are going to use this at the deployment
```bash
python run_senders_from_config.py --zmq --zmq-port 5555
```

**Detached mode:**
```bash
python run_senders_from_config.py --detach
```

#### ZMQ Protocol:
When using `--zmq`, the script listens for JSON messages:
```json
{
    "frame_id": "frame_camera01_000046836.jpg",
    "ball_id": "ball_001"
}
```

The script extracts the frame number and ball ID to construct appropriate start-after parameters and destination paths.

### 4. `camera_config.json`
**Camera configuration file**

JSON configuration defining camera sources and destination paths for the multi-camera system.

#### Structure:
```json
{
    "camera04": {
        "src": "/mnt/EyeQ_disk1/ring_buffer_jpg/data_transfer_test/camera04",
        "dest_path": "/mnt/bt3-disk-01/data_transfer_test/"
    },
    "camera05": {
        "src": "/mnt/EyeQ_disk1/ring_buffer_jpg/data_transfer_test/camera05",
        "dest_path": "/mnt/bt3-disk-02/data_transfer_test/"
    },
    "camera06": {
        "src": "/mnt/EyeQ_disk1/ring_buffer_jpg/data_transfer_test/camera06",
        "dest_path": "/mnt/bt3-disk-03/data_transfer_test/"
    }
}
```

#### Fields:
- `src`: Source directory path for camera images
- `dest_path`: Base destination path for transferred files

## Receiver Side Scripts

### 5. `run_recevier_cameras.py`
**Main orchestrator script for camera data reception**

This is the primary script that manages multiple camera receiver processes on the destination side.

#### Key Features:
- **Multi-process Architecture**: Starts multiple camera receivers with automatic restart
- **Port Management**: Assigns unique ports starting from 50001 for each camera
- **Worker Configuration**: Uses 16 worker processes per camera for high throughput
- **Graceful Shutdown**: Proper handling of process termination

#### Usage:
```bash
python3 run_recevier_cameras.py
```

**Features:**
- Starts camera receivers immediately based on `camera_config.json`
- Each camera gets assigned a unique port starting from 50001
- Uses 16 worker processes per camera
- Automatically restarts failed processes

### 6. `pyfast_recv_v2.py`
**High-performance TCP file receiver**

A multi-process TCP server that receives files from camera sources with support for destination path specification.

#### Key Features:
- **Multi-process Architecture**: High throughput with configurable worker processes
- **Protocol Support**: Both legacy and new destination-path protocols
- **Atomic File Writes**: Uses temporary files to prevent corruption
- **Connection Management**: Proper cleanup and error handling
- **Load Balancing**: SO_REUSEPORT support for efficient connection distribution

#### Usage:
```bash
python3 pyfast_recv_v2.py --port 50001 --out-dir /path/to/output --workers 16
```

#### Options:
- `--listen-ip`: IP address to bind to (default: 0.0.0.0)
- `--port`: Port number (required)
- `--out-dir`: Output directory (default: ./)
- `--workers`: Number of worker processes (default: 1)
- `--reuseport`: Enable SO_REUSEPORT for load balancing
- `--verbose`: Enable verbose logging
- `--expect-count-first`: Expect file count header per connection
- `--use-dest-paths`: Enable destination path support (new protocol)

**Protocol Support:**
- **Legacy Protocol**: Files stored directly in output directory
- **New Protocol**: Supports custom destination paths within output directory

### 7. `clear_destination.py`
**Directory cleanup utility**

Safely clears contents of destination directories with multiple operation modes and safety features.

#### Key Features:
- **Multiple Operation Modes**: Clear specific directories, camera-specific, or all cameras
- **Safety Validation**: Prevents dangerous operations on system directories
- **Confirmation Prompts**: Requires user confirmation unless bypassed
- **Configuration Integration**: Works with camera_config.json

#### Usage Examples:

**Clear specific directory:**
```bash
python3 clear_destination.py /path/to/destination [--yes]
```

**Clear camera-specific directory:**
```bash
python3 clear_destination.py --camera camera01 [--config /path/config.json] [--yes]
```

**Clear all camera directories:**
```bash
python3 clear_destination.py --all [--config /path/config.json] [--yes]
```

#### Options:
- `destination`: Direct path to directory to clear
- `--camera`: Camera name from config to clear
- `--all`: Clear all camera destinations from config
- `--config`: Path to camera_config.json (defaults to script directory)
- `--yes`, `-y`: Skip confirmation prompt

**Safety Features:**
- Validates destination exists and is a directory
- Refuses to operate on dangerous paths (e.g., root directory)
- Requires confirmation unless `--yes` is specified
- Preserves the destination directory itself

## System Architecture

### Complete Data Transfer Flow:
1. **Image Generation**: `random_image_generator.py` creates test images in camera directories
2. **Configuration**: `camera_config.json` defines source/destination mappings
3. **Receiver Setup**: `run_recevier_cameras.py` starts receiver processes on destination side
4. **Sender Orchestration**: `run_senders_from_config.py` launches multiple senders
5. **Data Transfer**: `pyfast_send_aftername_v2.py` sends files to `pyfast_recv_v2.py` receivers
6. **Cleanup**: `clear_destination.py` manages destination directory cleanup

### Sender Side Architecture:
- **File Discovery**: Polling-based detection with completion verification
- **File Waiting**: Intelligent waiting for files to appear before processing
- **Parallel Transfer**: Multi-threaded sending with configurable connections
- **Protocol Support**: TCP with custom framing for filenames and destination paths
- **Error Handling**: Retry logic and comprehensive error reporting

### Receiver Side Architecture:
- **Multi-process Servers**: Each camera gets dedicated receiver processes
- **Load Balancing**: SO_REUSEPORT for efficient connection distribution
- **Atomic Operations**: Temporary files prevent corruption during transfer
- **Directory Management**: Automatic directory creation and cleanup utilities

### Key Design Principles:
- **File Completion Detection**: Ensures files are fully written before transfer
- **File Existence Handling**: Waits for files to appear, preventing race conditions
- **Parallel Processing**: Multiple connections for high throughput
- **Fault Tolerance**: Retry logic and error handling
- **Flexible Routing**: Configurable destination paths with structure preservation
- **Integration Ready**: ZMQ support for external system integration

## Dependencies

### Python Packages:
- `pillow` - Image processing for test data generation
- `numpy` - Numerical operations for image generation
- `pyzmq` - ZMQ messaging (for trigger mode)

### System Requirements:
- **Python 3.6+** - Required for all scripts
- **Linux environment** - Tested on Ubuntu (recommended for production)
- **Network connectivity** - Between source and destination systems
- **Sufficient disk space** - For image generation, transfer, and storage
- **File permissions** - Read access to source directories, write access to destination directories
- **Standard library modules** - `argparse`, `json`, `os`, `socket`, `struct`, `sys`, `threading`, `time`, `multiprocessing`, `pathlib`, `shutil`

## Usage Examples

### Complete System Setup

**1. Generate test data:**
```bash
python random_image_generator.py
```

**2. Start receiver processes:**
```bash
python3 run_recevier_cameras.py
```

**3. Start sender processes (direct mode):**
```bash
python run_senders_from_config.py
```

**4. Start sender processes (ZMQ trigger mode):**
```bash
python run_senders_from_config.py --zmq --zmq-port 5555
```

### Individual Component Usage

**Start single camera receiver:**
```bash
python3 pyfast_recv_v2.py --port 50001 --out-dir /mnt/destination/ --workers 16 --verbose
```

**Send files from specific directory:**
```bash
python pyfast_send_aftername_v2.py \
    --src-dir /mnt/source/camera01 \
    --host 192.168.5.101 \
    --port 50001 \
    --pattern "*.jpg" \
    --conns 8 \
    --verbose
```

**Clear destination directories:**
```bash
# Clear all camera destinations
python3 clear_destination.py --all --yes

# Clear specific camera destination
python3 clear_destination.py --camera camera01 --yes

# Clear specific directory
python3 clear_destination.py /mnt/destination/camera01 --yes
```

### ZMQ Integration Example

**Send trigger message:**
```bash
# Using netcat to send ZMQ message
echo '{"frame_id": "frame_camera01_000046836.jpg", "ball_id": "ball_001"}' | nc localhost 5555
```

## Performance Considerations

### Sender Side:
- **Connection Count**: Default 8 connections provides good balance of throughput and resource usage
- **Lookahead**: Setting to 4 provides fast completion detection for sequential files
- **File Wait Time**: 10ms default wait time for files to appear balances responsiveness and efficiency
- **Chunk Size**: 8 MiB chunks optimize network transfer efficiency
- **Polling Interval**: 50ms default provides responsive file detection without excessive CPU usage

### Receiver Side:
- **Worker Processes**: Default 16 workers per camera for high throughput
- **SO_REUSEPORT**: Enables load balancing across multiple processes
- **Atomic Operations**: Prevents file corruption during concurrent transfers
- **Memory Usage**: Streaming transfers minimize memory footprint

## Error Handling

### Sender Side:
- Network connection retries with exponential backoff
- File system error recovery and completion detection
- File existence waiting with configurable timeouts
- Process timeout management
- Detailed error reporting with JSON output option
- Graceful shutdown on interruption

### Receiver Side:
- Automatic process restart on failure
- Connection error recovery and cleanup
- Safe directory operations with validation
- Comprehensive error logging
- Graceful shutdown on SIGINT/SIGTERM

## Monitoring and Debugging

### Sender Side:
- Use `--verbose` flag for detailed operation logs
- `--json-stats` provides machine-readable performance statistics
- Error queues capture and report transfer failures
- Process monitoring available in detached mode

### Receiver Side:
- Verbose logging for connection and transfer details
- Process monitoring with automatic restart
- Connection statistics and error reporting
- Safe file operations with temporary file handling

## Security Features

- **Path Validation**: Prevents dangerous operations on system directories
- **Confirmation Prompts**: Requires user confirmation for destructive operations
- **Safe File Handling**: Uses temporary files to prevent corruption
- **Input Validation**: Validates configuration files and parameters
- **Network Security**: Configurable IP binding and port management
