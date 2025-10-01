#!/usr/bin/env python3
"""
Kaggle Dataset Downloader using Docker
Downloads the real coffee shop transaction dataset from Kaggle using Docker
"""

import os
import sys
import subprocess
import json
import shutil
from pathlib import Path

def print_status(message):
    """Print status message with blue color"""
    print(f"\033[0;34m[INFO]\033[0m {message}")

def print_success(message):
    """Print success message with green color"""
    print(f"\033[0;32m[SUCCESS]\033[0m {message}")

def print_warning(message):
    """Print warning message with yellow color"""
    print(f"\033[1;33m[WARNING]\033[0m {message}")

def print_error(message):
    """Print error message with red color"""
    print(f"\033[0;31m[ERROR]\033[0m {message}")

def check_docker():
    """Check if Docker is available and running"""
    try:
        result = subprocess.run(['docker', '--version'], 
                              capture_output=True, text=True, check=True)
        print_status(f"Docker found: {result.stdout.strip()}")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print_error("Docker is not available. Please install Docker and try again.")
        return False

def check_kaggle_credentials():
    """Check if Kaggle credentials are available"""
    kaggle_dir = Path.home() / '.kaggle'
    kaggle_json = kaggle_dir / 'kaggle.json'
    
    if not kaggle_json.exists():
        print_error("Kaggle credentials not found!")
        print_status("Please follow these steps:")
        print_status("1. Go to https://www.kaggle.com/account")
        print_status("2. Create API Token (downloads kaggle.json)")
        print_status("3. Move kaggle.json to ~/.kaggle/kaggle.json")
        print_status("4. Set permissions: chmod 600 ~/.kaggle/kaggle.json")
        return False
    
    # Check if the file is readable and contains valid JSON
    try:
        with open(kaggle_json, 'r') as f:
            creds = json.load(f)
        if 'username' not in creds or 'key' not in creds:
            print_error("Invalid kaggle.json format. Missing username or key.")
            return False
        print_success("Kaggle credentials found")
        return True
    except (json.JSONDecodeError, IOError) as e:
        print_error(f"Error reading kaggle.json: {e}")
        return False

def create_dockerfile():
    """Create a Dockerfile for the Kaggle downloader"""
    dockerfile_content = """FROM python:3.9-slim

# Install required packages
RUN pip install kaggle pandas

# Create working directory
WORKDIR /app

# Copy kaggle credentials
COPY kaggle.json /root/.kaggle/kaggle.json
RUN chmod 600 /root/.kaggle/kaggle.json

# Create data directory
RUN mkdir -p /app/data

# Download script
COPY download_script.py /app/download_script.py

# Run the download
CMD ["python", "download_script.py"]
"""
    
    with open('Dockerfile.kaggle', 'w') as f:
        f.write(dockerfile_content)
    print_status("Created Dockerfile.kaggle")

def create_download_script():
    """Create the Python script that will run inside Docker"""
    download_script = """#!/usr/bin/env python3
import kaggle
import pandas as pd
import os
import sys

def main():
    print("Starting Kaggle dataset download...")
    
    # Dataset information
    dataset_name = "geraldooizx/g-coffee-shop-transaction-202307-to-202506"
    output_dir = "/app/data"
    
    try:
        # Download the dataset
        print(f"Downloading dataset: {dataset_name}")
        kaggle.api.dataset_download_files(
            dataset_name, 
            path=output_dir, 
            unzip=True
        )
        
        # Find the CSV file
        csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
        
        if not csv_files:
            print("ERROR: No CSV files found in downloaded dataset")
            sys.exit(1)
        
        # Use the first CSV file found
        csv_file = csv_files[0]
        csv_path = os.path.join(output_dir, csv_file)
        
        # Rename to our expected filename
        target_path = os.path.join(output_dir, "coffee_shop_transactions.csv")
        if csv_path != target_path:
            os.rename(csv_path, target_path)
            print(f"Renamed {csv_file} to coffee_shop_transactions.csv")
        
        # Verify the file
        if os.path.exists(target_path):
            # Count lines (excluding header)
            with open(target_path, 'r') as f:
                lines = f.readlines()
            record_count = len(lines) - 1  # Exclude header
            
            print(f"SUCCESS: Dataset downloaded successfully!")
            print(f"File: {target_path}")
            print(f"Records: {record_count}")
            print(f"Size: {os.path.getsize(target_path)} bytes")
        else:
            print("ERROR: Target file not found after processing")
            sys.exit(1)
            
    except Exception as e:
        print(f"ERROR: Failed to download dataset: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
"""
    
    with open('download_script.py', 'w') as f:
        f.write(download_script)
    print_status("Created download_script.py")

def run_docker_download():
    """Run the Docker container to download the dataset"""
    print_status("Building Docker image for Kaggle download...")
    
    # Build the Docker image
    build_cmd = [
        'docker', 'build', 
        '-f', 'Dockerfile.kaggle',
        '-t', 'kaggle-downloader',
        '.'
    ]
    
    try:
        subprocess.run(build_cmd, check=True)
        print_success("Docker image built successfully")
    except subprocess.CalledProcessError as e:
        print_error(f"Failed to build Docker image: {e}")
        return False
    
    print_status("Running Docker container to download dataset...")
    
    # Run the container with volume mount
    run_cmd = [
        'docker', 'run', 
        '--rm',
        '-v', f'{os.path.abspath("../data")}:/app/data',
        'kaggle-downloader'
    ]
    
    try:
        result = subprocess.run(run_cmd, check=True, capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print_warning(f"Container stderr: {result.stderr}")
        return True
    except subprocess.CalledProcessError as e:
        print_error(f"Failed to run Docker container: {e}")
        if e.stdout:
            print(f"Container stdout: {e.stdout}")
        if e.stderr:
            print(f"Container stderr: {e.stderr}")
        return False

def cleanup():
    """Clean up temporary files"""
    temp_files = ['Dockerfile.kaggle', 'download_script.py']
    for file in temp_files:
        if os.path.exists(file):
            os.remove(file)
            print_status(f"Cleaned up {file}")

def main():
    """Main function"""
    print_status("Starting Kaggle dataset download using Docker...")
    
    # Check prerequisites
    if not check_docker():
        sys.exit(1)
    
    if not check_kaggle_credentials():
        sys.exit(1)
    
    # Create data directory
    data_dir = Path("../data")
    data_dir.mkdir(exist_ok=True)
    print_status(f"Data directory: {data_dir.absolute()}")
    
    try:
        # Create necessary files
        create_dockerfile()
        create_download_script()
        
        # Copy kaggle credentials to current directory
        kaggle_json = Path.home() / '.kaggle' / 'kaggle.json'
        shutil.copy2(kaggle_json, 'kaggle.json')
        print_status("Copied kaggle.json to current directory")
        
        # Run Docker download
        if run_docker_download():
            print_success("Dataset download completed successfully!")
            
            # Verify the downloaded file
            dataset_file = data_dir / "coffee_shop_transactions.csv"
            if dataset_file.exists():
                with open(dataset_file, 'r') as f:
                    lines = f.readlines()
                record_count = len(lines) - 1  # Exclude header
                file_size = dataset_file.stat().st_size
                
                print_success(f"Dataset verified:")
                print_success(f"  File: {dataset_file}")
                print_success(f"  Records: {record_count}")
                print_success(f"  Size: {file_size:,} bytes")
            else:
                print_error("Dataset file not found after download")
                sys.exit(1)
        else:
            print_error("Dataset download failed")
            sys.exit(1)
            
    except Exception as e:
        print_error(f"Unexpected error: {e}")
        sys.exit(1)
    finally:
        cleanup()

if __name__ == "__main__":
    main()
