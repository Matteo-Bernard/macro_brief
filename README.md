
# News Copilot – Automated Market and Macro Newsletter

**News Copilot** is a Python-based tool designed to automate the generation and delivery of financial and macroeconomic newsletters. It compiles structured tables summarizing key market data and macro indicators, intended for financial analysts, portfolio managers, and economic researchers.

---

## Overview

The newsletter provides a concise overview of:

- Recent market movements across major asset classes
- Key U.S. macroeconomic indicators (via the FRED API)
- Custom metrics such as rolling volume averages and annualized volatility

The output is formatted as an HTML email, sent automatically to a predefined list of recipients.

---

## Contents of the Newsletter

### 1. **Multi-Asset Market Dashboard**

A structured table covering performance metrics for key asset classes:

- Equities (e.g. S&P 500, Nasdaq)
- Commodities (e.g. Gold, Oil)
- Government bonds (e.g. US 10Y)
- FX (e.g. EUR/USD, DXY)
- Crypto (e.g. Bitcoin)

Metrics typically include:

- Last available level
- Daily, 5-day, 1-month, YTD, and 1-year returns
- Data is adjusted for business days when appropriate

### 2. **Macroeconomic Indicators**

Pulled from the FRED API and presented in table form:

- Inflation (e.g. CPI YoY)
- Employment (e.g. Unemployment rate, NFP)
- Interest rates (e.g. EFFR)
- Consumption and industrial data (e.g. ISM, Retail Sales)

Each indicator includes its current value and historical references (1M, 3M, 6M, 1Y ago).

### 3. **Custom Metrics**

Examples include:

- Bitcoin rolling 50-day average volume
- Volume variation from the current average
- Annualized historical volatility (252-day window)

All figures are formatted and aligned for direct interpretation.

---

## Technical Stack

- **Data sources**: Yahoo Finance, FRED API
- **Libraries**: `pandas`, `numpy`, `datetime`, `smtplib`, `email`, `ssl`
- **Email output**: HTML format with optional BCC handling
- **Customization**: Pipelines can be easily modified to track any time series or apply custom transformations

---

## File Structure

- `news_copilot.ipynb`: Main notebook containing the full pipeline
- `.env`: Stores confidential information (API keys, sender credentials)
- `metadata.json`: Defines sender and recipients
- `README.md`: Documentation

---

## Usage

1. Define your data sources and transformations via the `pipeline` dictionary
2. Run the notebook to generate and send the newsletter
3. Output is delivered by email in HTML format, without exposing recipient list (via BCC)

---

## Example Use Cases

- Daily internal market updates for investment teams  
- Weekly macro dashboards for clients or management  
- Automated monitoring of asset-specific indicators (e.g. Bitcoin volatility)
