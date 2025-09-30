# Kaggle Dataset Setup Guide

To use the **real Kaggle dataset** instead of the sample data, follow these steps:

## 1. Get Kaggle API Credentials

1. **Go to Kaggle**: Visit [https://www.kaggle.com/account](https://www.kaggle.com/account)
2. **Sign in** to your Kaggle account (create one if you don't have it)
3. **Create API Token**: 
   - Scroll down to the "API" section
   - Click **"Create New API Token"**
   - This will download a file called `kaggle.json`

## 2. Set Up Credentials

1. **Create the kaggle directory**:
   ```bash
   mkdir -p ~/.kaggle
   ```

2. **Move the downloaded file**:
   ```bash
   mv ~/Downloads/kaggle.json ~/.kaggle/kaggle.json
   ```

3. **Set proper permissions**:
   ```bash
   chmod 600 ~/.kaggle/kaggle.json
   ```

## 3. Download the Real Dataset

Once credentials are set up, run:

```bash
cd tests
./setup_real_dataset.sh
```

This will:
- ✅ Download the real Kaggle coffee shop dataset
- ✅ Process 100,000+ real transactions
- ✅ Set up the dataset for testing

## 4. Run the Real Dataset Test

```bash
./run_real_dataset_test.sh
```

## What You'll Get

- **Real Dataset**: Actual coffee shop transaction data from Kaggle
- **100,000+ Records**: Real transaction patterns and data
- **Realistic Testing**: Test with actual data structure and patterns
- **Performance Metrics**: Real processing times and throughput

## Troubleshooting

### "Kaggle credentials not found"
- Make sure `~/.kaggle/kaggle.json` exists
- Check file permissions: `ls -la ~/.kaggle/`
- Verify the file contains valid JSON

### "Download failed"
- Check your internet connection
- Verify the dataset is public: [g-coffee-shop-transaction-202307-to-202506](https://www.kaggle.com/datasets/geraldooizx/g-coffee-shop-transaction-202307-to-202506)
- Try running the command manually: `kaggle datasets download -d geraldooizx/g-coffee-shop-transaction-202307-to-202506`

### "Permission denied"
- Make sure you have Docker installed and running
- Check if you can run Docker commands: `docker --version`

## Current Status

Right now, the system is using a **realistic sample dataset** with 100,000 transactions that mimics real coffee shop patterns. This is perfect for testing the system architecture and performance.

To get the **actual Kaggle dataset**, just follow the credential setup steps above!
