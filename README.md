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
The purpose of this repository is to compare the effectiveness of using a smart algorithm to count transaction volume. Limiting the transaction volume
could be a useful tool in game theoretic blockchain protocols. Instead of counting the total transaction volume a smarter algorithm could be deployed that
captures a more realistic loss of value that is possible in a given timeframe Δ
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
You need access to a Validator node and a coin gecko api key for the daily price of the erc-20 tokens.
In the .env file you need to set theses keys. The price data to determine the value of each transaction is taken daily using a CoinGecko.
```
RCP_URL="localhost:5000"
COIN_GECKO_API_KEY="xxx"
```
Run this script to save transaction data into a duckdb database. In the config file you can change various parameters, 
add ERC-20 token transfers, mark transactions with certain events as DEX transactions and choose the blocks to be saved.
```bash
python3 ./src/collect_main.py
```
Run this script to save data into a duckdb database and then plot the two transaction counting algorithms with different window sizes to .svg files. 
```bash
python3 ./src/collect_and_plot_main.py
```
---
## Attribution
Price data powered by [CoinGecko API](https://www.coingecko.com)

