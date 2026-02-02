# Measuring Transaction Volume.
Analysis of transaction streams between wallets. Compares the tracked transaction volume between two algorithms and plots native Ethereum and ERC-20 transactions.

---
## Table of Contents
- Overview  
- Installation  
- Usage  
- Attribution
---

## Overview

Information about the total transaction volume on the Ethereum blockchain can be used in many different areas. This repository includes
- RPC Client code to extract data from a to you available Ethereum validator node. 
- Two different algorithms used to compute the transaction volume
- Plotting tools to visualise the results
---

## Installation
```bash
git clone https://github.com/clemensJul/measuring-transaction-volume
cd measuring-transaction-volume
cp .env.example .env
python3 -m venv .venv
source ./.venv/bin/activate
python3 pip install -r requirements.txt
```

---
## Usage
You can either analyze Ethereum native tokens or ERC-20 tokens. The transaction volume is converted to USD using one price per day.

In ./config/ you can set ERC-20 token transactions collected. Ethereum blocks looked at and other configs.

You need access to a Validator node and a coin gecko api key for the daily price of the erc-20 tokens.
```
RCP_URL="localhost:5000"
COIN_GECKO_API_KEY="xxx"
```
---
## Attribution
Price data powered by [CoinGecko API](https://www.coingecko.com)

