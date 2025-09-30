#!/usr/bin/env python3
"""
Script to download the Kaggle Coffee Shop Transaction dataset
and convert it to a format suitable for our Go tests.
"""

import os
import sys
import time
import pandas as pd
from pathlib import Path

def download_kaggle_dataset():
    """Download the Kaggle dataset using the Kaggle API"""
    try:
        import kaggle
    except ImportError:
        print("Kaggle API not installed. Installing...")
        os.system("pip install kaggle")
        import kaggle
    
    print("Downloading Kaggle dataset: geraldooizx/g-coffee-shop-transaction-202307-to-202506")
    
    # Create data directory
    data_dir = Path("../data")
    data_dir.mkdir(exist_ok=True)
    
    try:
        # Download the dataset
        kaggle.api.dataset_download_files(
            'geraldooizx/g-coffee-shop-transaction-202307-to-202506',
            path=str(data_dir),
            unzip=True
        )
        
        print("Dataset downloaded successfully!")
        
        # Find the CSV file
        csv_files = list(data_dir.glob("*.csv"))
        if not csv_files:
            print("No CSV files found in the downloaded dataset")
            return None
            
        csv_file = csv_files[0]
        print(f"Found CSV file: {csv_file}")
        
        # Load and display basic info about the dataset
        df = pd.read_csv(csv_file)
        print(f"Dataset shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(f"First few rows:")
        print(df.head())
        
        # Save a sample for testing
        sample_file = data_dir / "coffee_shop_transactions_sample.csv"
        df.head(1000).to_csv(sample_file, index=False)
        print(f"Sample saved to: {sample_file}")
        
        return str(csv_file)
        
    except Exception as e:
        print(f"Error downloading dataset: {e}")
        print("Make sure you have:")
        print("1. Kaggle API credentials in ~/.kaggle/kaggle.json")
        print("2. The dataset is public or you have access to it")
        return None

def main():
    print("Kaggle Dataset Downloader for Coffee Shop Transactions")
    print("=" * 60)
    
    # Try to download from Kaggle first
    dataset_file = download_kaggle_dataset()
    
    if dataset_file:
        print(f"\nDataset ready at: {dataset_file}")
        print("You can now run the Go tests with this dataset!")
    else:
        print("Failed to create dataset")
        sys.exit(1)

if __name__ == "__main__":
    main()
