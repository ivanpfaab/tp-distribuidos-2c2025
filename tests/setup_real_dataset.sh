#!/bin/bash

# Real Dataset Setup Script
# This script downloads the real Kaggle dataset and sets up Docker volumes

set -e

echo "=========================================="
echo "Real Dataset Setup for Kaggle Coffee Shop"
echo "=========================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Create data directory
mkdir -p ../data

# Check if dataset already exists
if [ -f "../data/coffee_shop_transactions.csv" ]; then
    print_warning "Dataset already exists. Do you want to re-download? (y/N)"
    read -r response
    if [[ ! "$response" =~ ^[Yy]$ ]]; then
        print_status "Using existing dataset"
        exit 0
    fi
fi

# Try to download from Kaggle first
print_status "Attempting to download real Kaggle dataset using Docker..."

# Use the Docker-based Python script
python3 download_kaggle_docker.py

# Verify dataset
if [ -f "../data/coffee_shop_transactions.csv" ]; then
    record_count=$(wc -l < ../data/coffee_shop_transactions.csv)
    file_size=$(du -h ../data/coffee_shop_transactions.csv | cut -f1)
    print_success "Dataset ready:"
    print_success "  File: ../data/coffee_shop_transactions.csv"
    print_success "  Records: $((record_count - 1)) (excluding header)"
    print_success "  Size: $file_size"
else
    print_error "Failed to create dataset"
    exit 1
fi

print_success "Dataset setup completed!"
