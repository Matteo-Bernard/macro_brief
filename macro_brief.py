# %% [markdown]
# ## 1. Introduction et Conclusion

# %%
import datetime as dt
synthesis = f"""
<table style="font-family: Segoe UI, Tahoma, sans-serif; font-size: 16px;">
        <tr><td><strong>Date</strong></td><td>: {dt.datetime.now().strftime("%d-%m-%Y")}</td></tr>
        <tr><td><strong>Objectif</strong></td><td>: Newsletter quotidienne de l'actualité financière et économique mondiale</td></tr>
        <tr><td><strong>Créateur</strong></td><td>: <a href=https://www.linkedin.com/in/matt%C3%A9o-bernard/>Mattéo Bernard</a></td></tr>
        <tr><td><strong>Github</strong></td><td>: <a href=https://github.com/Matteo-Bernard/macro_brief>Macro Brief</a></td></tr>
        <tr><td><strong>Note n°1</strong></td><td>: Ce document ne doit être utilisé pour la prise de décision financière</td></tr>
        <tr><td><strong>Note n°2</strong></td><td>: Le projet est en cours de développement, tout commentaire est bon à prendre :)</td></tr>
</table>
"""

source = f"""
<table style="font-family: Segoe UI, Tahoma, sans-serif; font-size: 16px;">
        <tr><td><strong>Météo</strong></td><td>:\
                   <a href=https://openweathermap.org/>OpenWeather</a></td></tr>
        <tr><td><strong>Agent IA</strong></td><td>:\
                   <a href=https://mistral.ai/news/mistral-medium-3/>Mistral Medium 3</a></td></tr>
        <tr><td><strong>Données US</strong></td><td>:\
                   <a href=https://fr.finance.yahoo.com/>Yahoo Finance</a>\
                 / <a href=https://fred.stlouisfed.org//>Federal Reserve Bank of Saint Louis</a>\
                 / <a href=https://www.newyorkfed.org/>Federal Reserve Bank of New York</a></td></tr>
        <tr><td><strong>Données EU</strong></td><td>:\
                   <a href=https://www.ecb.europa.eu/home/html/index.en.html>Banque Centrale Européenne</a>\
                 / <a href=https://www.banque-france.fr/en>Banque de France</a>\
                 / <a href=https://www.bundesbank.de/en>Deutsche Bundesbank</a></td></tr>
        <tr><td><strong>Matières premières</strong></td><td>:\
                   <a href=https://www.eia.gov/>US Energy Information Administration</a></td></tr>
        <tr><td><strong>Articles</strong></td><td>:\
                   <a href=https://ft.com>Financial Times</a>\
                 / <a href=https://www.lemonde.fr/>Le Monde</a></td></tr>
        <tr><td><strong>Fear and Greed</strong></td><td>:\
                   <a href=https://edition.cnn.com/markets/fear-and-greed>CNN</a></td></tr>
</table>
"""

# %% [markdown]
# ## 2. Partie Tableaux

# %%
import pandas as pd
import numpy as np
import datetime as dt

def call_pipeline(data, pipeline, timeframes, today=None):
    today = today.to_pydatetime().date() if today else dt.date.today()
    ref_info = {
        today: today,
        '1D':   today - dt.timedelta(days=1),
        '5D':   today - dt.timedelta(days=5),
        '1M':   today - dt.timedelta(days=30),
        '6M':   today - dt.timedelta(days=180),
        'YTD':  dt.date(today.year, 1, 1),
        '1Y':   today - dt.timedelta(days=360),
    }

    df = pd.DataFrame(
        {
            pipe['label']: {
                tf: (
                    pipe['transform'](data)
                    .dropna()
                    .sort_index()
                    .loc[lambda s: s.index[s.index <= pd.to_datetime(ref_info[tf])].max()]
                )
                if tf in ref_info else np.nan
                for tf in timeframes
            }
            for pipe in pipeline
        }
    )

    df = pd.DataFrame({
        pipe['label']: (
            df[pipe['label']]
            .replace(0, '-')
            .map(lambda x: format(x, pipe['format']) if isinstance(x, (int, float)) else x)
            if 'format' in pipe else df[pipe['label']].replace(0, '-')
        )
        for pipe in pipeline
    })

    return df.T


# %%
import pandas as pd

def format_html(df):
    html_table = df.reset_index().to_html(classes='table-style', index=False, escape=False)

    html_with_css = f"""
    <style>
        .table-style {{
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            font-size: 14px;
            color: #333;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
        }}
        .table-style th, .table-style td {{
            padding: 12px 16px;
            border-bottom: 1px solid #eee;
            text-align: center;
            background-color: #ffffff;
        }}
        .table-style th {{
            background-color: #f5f7fa;
            font-weight: 600;
        }}
    </style>
    {html_table}
    """
    return html_with_css


# %% [markdown]
# ### Météo

# %%
from settings import OPENWEATHER_KEY
import pandas as pd
import requests
import datetime as dt

lat = 48.86
lon = 2.33
key = OPENWEATHER_KEY
units = 'metric'
lang = 'en'
weather_url = f'https://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={key}&units={units}&lang={lang}'
r = requests.get(url=weather_url)
weather_json = r.json()

index = ['Météo', 'Temperature', 'Ressenti', 'Humidité', 'Vent']
weather_df = pd.DataFrame(index=index)

for dict in weather_json['list']:
    if int(dict['dt']) < int((dt.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + dt.timedelta(days=1)).timestamp()):
        weather_time = dt.datetime.fromtimestamp(int(dict['dt'])).strftime('%Y-%m-%d %H:%M')
        weather_df.loc['Météo', weather_time] = dict['weather'][0]['description']
        weather_df.loc['Temperature', weather_time] = f"{dict['main']['temp']:.0f} °C"
        weather_df.loc['Ressenti', weather_time] = f"{dict['main']['feels_like']:.0f} °C"
        weather_df.loc['Humidité', weather_time] = f"{dict['main']['humidity']:.0f} %"
        weather_df.loc['Vent', weather_time] = f"{dict['wind']['speed']:.1f} km/h"
        weather_df.loc['Pression', weather_time] = f"{dict['main']['pressure']:.0f} hPa"

# %% [markdown]
# ### Actions

# %%
import yfinance as yf
import pandas as pd
import numpy as np

market_args = {
    '^FCHI' : ['Close'],
    '^GSPC' : ['Close'],
    '^IXIC' : ['Close'],
    '^N225' : ['Close'],
    '^HSI'  : ['Close']
}

market_data = pd.DataFrame()
for symbol, args in market_args.items():
    data = yf.Ticker(symbol).history(period='5y')
    data.index = pd.to_datetime(data.index)
    data.index = data.index.tz_localize(None)    
    for arg in args:
        market_data[f'{symbol} {arg}'] = data[arg]

market_data = market_data.dropna()

# %%
market_pip = [
    {
        'label': 'CAC 40',
        'transform': lambda df: df['^FCHI Close'],
        'format' : ',.2f'
    },
    {
        'label': 'CAC 40 Total Return',
        'transform': lambda df: np.log(df['^FCHI Close'].iloc[-1] / df['^FCHI Close']),
        'format' : '+.2%'
    },
    {
        'label': 'S&P 500',
        'transform': lambda df: df['^GSPC Close'],
        'format' : ',.2f'
    },
    {
        'label': 'S&P 500 Total Return',
        'transform': lambda df: np.log(df['^GSPC Close'].iloc[-1] / df['^GSPC Close']),
        'format' : '+.2%'
    },
    {
        'label': 'NASDAQ 100',
        'transform': lambda df: df['^IXIC Close'],
        'format' : ',.2f'
    },
    {
        'label': 'NASDAQ 100 Total Return',
        'transform': lambda df: np.log(df['^IXIC Close'].iloc[-1] / df['^IXIC Close']),
        'format' : '+.2%'
    },
    {
        'label': 'NIKKEI 225',
        'transform': lambda df: df['^N225 Close'],
        'format' : ',.2f'
    },
    {
        'label': 'NIKKEI 225 Total Return',
        'transform': lambda df: np.log(df['^N225 Close'].iloc[-1] / df['^N225 Close']),
        'format' : '+.2%'
    },
    {
        'label': 'Hang Seng Index',
        'transform': lambda df: df['^HSI Close'],
        'format' : ',.2f'
    },
    {
        'label': 'HSI Total Return',
        'transform': lambda df: np.log(df['^HSI Close'].iloc[-1] / df['^HSI Close']),
        'format' : '+.2%'
    },
]

today = market_data.index[-1].to_pydatetime().date()
timeframes = [today, '1D', '5D', '1M', 'YTD', '1Y']

market_df = call_pipeline(market_data, market_pip, timeframes, today=market_data.index[-1])

# %%
market_df

# %% [markdown]
# ### Devises

# %%
import yfinance as yf
import pandas as pd

currency_args = {
    'EURUSD=X'  : ['Close'],
    'GBPUSD=X'  : ['Close'],
    'JPY=X'     : ['Close'],
    'CNY=X'     : ['Close'],
    'CHF=X'     : ['Close']
}

currency_data = pd.DataFrame()
for symbol, args in currency_args.items():
    data = yf.Ticker(symbol).history(period='5y')
    data.index = pd.to_datetime(data.index)
    data.index = data.index.tz_localize(None)
    for arg in args:
        currency_data[f'{symbol} {arg}'] = data[arg]
currency_data = currency_data.dropna()

# %%
currency_pip = [
    {
        'label': 'EUR/USD',
        'transform': lambda df: df['EURUSD=X Close'],
        'format' : ',.4f'
    },
    {
        'label': 'EUR/USD Total Return',
        'transform': lambda df: np.log(df['EURUSD=X Close'].iloc[-1] / df['EURUSD=X Close']),
        'format' : '+.2%'
    },
    {
        'label': 'EUR/USD Volatility',
        'transform': lambda df: np.log(df['EURUSD=X Close'] / df['EURUSD=X Close'].shift(1)).rolling(30).std() * np.sqrt(365),
        'format': '.2%'
    },
    {
        'label': 'GBP/USD',
        'transform': lambda df: df['GBPUSD=X Close'],
        'format' : ',.2f'
    },
    {
        'label': 'USD/JPY',
        'transform': lambda df: df['JPY=X Close'],
        'format' : ',.0f'
    },
    {
        'label': 'USD/CNY',
        'transform': lambda df: df['CNY=X Close'],
        'format' : ',.2f'
    },
    {
        'label': 'USD/CHF',
        'transform': lambda df: df['CHF=X Close'],
        'format' : ',.2f'
    },
]

today = currency_data.index[-1].to_pydatetime().date()
timeframes = [today, '1D', '5D', '1M', 'YTD', '1Y']

currency_df = call_pipeline(currency_data, currency_pip, timeframes, today=currency_data.index[-1])

# %%
currency_df

# %% [markdown]
# ### Crypto Actifs

# %%
from settings import BINANCE_KEY, BINANCE_SECRET
from EcoWatch.Scraping import binance
import pandas as pd
import numpy as np

start = (dt.datetime.today() - dt.timedelta(days=5000)).strftime('%Y/%m/%d')
end = dt.datetime.today().strftime('%Y/%m/%d')

crypto_args = {
    'BTCUSDT' : ['Close', 'Volume'],
    'ETHUSDT' : ['Close', 'Volume'],
    'SOLUSDT' : ['Close', 'Volume']
}

crypto_data = pd.DataFrame()
for symbol, args in crypto_args.items():
    data = binance(BINANCE_KEY, BINANCE_SECRET, symbol, start, end)
    for arg in args:
        crypto_data[f'{symbol} {arg}'] = data[arg]
crypto_data = crypto_data.dropna()

# %%
crypto_pip = [
    {
        'label': 'Bitcoin (USD)',
        'transform': lambda df: df['BTCUSDT Close'],
        'format' : ',.2f'
    },
    {
        'label': 'Bitcoin Total Return',
        'transform': lambda df: np.log(df['BTCUSDT Close'].iloc[-1] / df['BTCUSDT Close']),
        'format' : '+.2%'
    },
    {
        'label': 'Bitcoin Volatility',
        'transform': lambda df: np.log(df['BTCUSDT Close'] / df['BTCUSDT Close'].shift(1)).rolling(30).std() * np.sqrt(365),
        'format': '.2%'
    },
    {
        'label': 'Bitcoin Volume (MA30)',
        'transform': lambda df: df['BTCUSDT Volume'].rolling(30).mean(),
        'format' : ',.2f'
    },
    {
        'label': 'Bitcoin Volume Var. (MA30)',
        'transform': lambda df: df['BTCUSDT Volume'].rolling(30).mean().iloc[-1] - df['BTCUSDT Volume'].rolling(30).mean(),
        'format' : '+,.2f'
    },
    {
        'label': 'Etherum (USD)',
        'transform': lambda df: df['ETHUSDT Close'],
        'format' : ',.2f'
    },
    {
        'label': 'Etherum Total Return',
        'transform': lambda df: np.log(df['ETHUSDT Close'].iloc[-1] / df['ETHUSDT Close']),
        'format' : '+.2%'
    },
    {
        'label': 'Solana (USD)',
        'transform': lambda df: df['SOLUSDT Close'],
        'format' : ',.2f'
    },
    {
        'label': 'Solana Total Return',
        'transform': lambda df: np.log(df['SOLUSDT Close'].iloc[-1] / df['SOLUSDT Close']),
        'format' : '+.2%'
    },
]

today = crypto_data.index[-1].to_pydatetime().date()
timeframes = [today, '1D', '5D', '1M', 'YTD', '1Y']

crypto_df = call_pipeline(crypto_data, crypto_pip, timeframes, today=crypto_data.index[-1])

# %%
crypto_df

# %% [markdown]
# ### Alternatifs

# %%
import yfinance as yf
import pandas as pd

alter_args = {
    'GC=F'      : ['Close'],
    'IEMG'      : ['Close'],
    'HPRD.L'    : ['Close'],
    'PSP'       : ['Close'],
    'IGF'       : ['Close']
}

alter_data = pd.DataFrame()
for symbol, args in alter_args.items():
    data = yf.Ticker(symbol).history(period='5y')
    data.index = pd.to_datetime(data.index)
    data.index = data.index.tz_localize(None)
    for arg in args:
        alter_data[f'{symbol} {arg}'] = data[arg]
alter_data = alter_data.dropna()

# %%
alter_pip = [
    {
        'label': 'Future Gold ($/oz)',
        'transform': lambda df: df['GC=F Close'],
        'format' : ',.2f'
    },
    {
        'label': 'Future Gold (%)',
        'transform': lambda df: np.log(df['GC=F Close'].iloc[-1] / df['GC=F Close']),
        'format' : '+.2%'
    },
    {
        'label': 'Emerging Markets (%)',
        'transform': lambda df: np.log(df['IEMG Close'].iloc[-1] / df['IEMG Close']),
        'format' : '+.2%'
    },
    {
        'label': 'Real Estate (%)',
        'transform': lambda df: np.log(df['HPRD.L Close'].iloc[-1] / df['HPRD.L Close']),
        'format' : '+.2%'
    },
    {
        'label': 'Private Equity (%)',
        'transform': lambda df: np.log(df['PSP Close'].iloc[-1] / df['PSP Close']),
        'format' : '+.2%'
    },
    {
        'label': 'Infrastructures (%)',
        'transform': lambda df: np.log(df['IGF Close'].iloc[-1] / df['IGF Close']),
        'format' : '+.2%'
    },
]

today = alter_data.index[-1].to_pydatetime().date()
timeframes = [today, '1D', '5D', '1M', 'YTD', '1Y']

alter_df = call_pipeline(alter_data, alter_pip, timeframes, today=alter_data.index[-1])

alter_table = """<table style="font-family: Segoe UI, Tahoma, sans-serif; font-size: 14px;">
    <tr><td><strong>Emerging Markets</strong></td><td>: Indice MSCI Emerging Markets (Proxi : iShares Core MSCI Emerging Markets ETF).</td></tr>
    <tr><td><strong>Real Estate</strong></td><td>: Indice FTSE EPRA/NAREIT Developed (Proxi : HSBC FTSE EPRA NAREIT Developed UCITS ETF).</td></tr>
    <tr><td><strong>Private Equity</strong></td><td>: Indice Red Rocks Global Listed Private Equity Index (Proxi : Invesco Global Listed Private Equity ETF).</td></tr>
    <tr><td><strong>Infrastructure</strong></td><td>: Indice S&P Global Infrastructure Index (Proxi : iShares Global Infrastructure ETF).</td></tr>
</table>"""

# %%
alter_df

# %% [markdown]
# ### Matières premières

# %%
from settings import EIA_KEY
from EcoWatch.Scraping import eia
import pandas as pd
import numpy as np

raw_args = {
    'Spot BRENT': {
        'route': 'petroleum', 
        'contract': 'spt', 
        'product': 'EPCBRENT'
    },
    'Spot Natural Gas': {
        'route': 'natural-gas', 
        'contract': 'fut', 
        'product': 'EPG0'
    },
    'Spot WTI': {
        'route': 'petroleum', 
        'contract': 'spt', 
        'product': 'EPCWTI'
    },
}

raw_data = pd.DataFrame(columns=raw_args.keys())
for series, args in raw_args.items():
    raw_data[series] = eia(EIA_KEY, args['route'], args['contract'], args['product'])

# %%
raw_pip = [
    {
        'label': 'Spot BRENT ($/BBL)',
        'transform': lambda df: df['Spot BRENT'],
        'format' : '.2f'
    },
    {
        'label': 'Spot BRENT Total Return',
        'transform': lambda df: np.log(df['Spot BRENT'].iloc[-1] / df['Spot BRENT']),
        'format' : '+.2%'
    },
    {
        'label': 'Spot BRENT Volatility',
        'transform': lambda df: np.log(df['Spot BRENT'] / df['Spot BRENT'].shift(1)).rolling(30).std() * np.sqrt(365),
        'format' : '.2%'
    },
    {
        'label': 'Spot WTI ($/BBL)',
        'transform': lambda df: df['Spot WTI'],
        'format' : '.2f'
    },
    {
        'label': 'Spot WTI Total Return',
        'transform': lambda df: np.log(df['Spot WTI'].iloc[-1] / df['Spot WTI']),
        'format' : '+.2%'
    },
    {
        'label': 'Spot WTI Volatility',
        'transform': lambda df: np.log(df['Spot WTI'] / df['Spot WTI'].shift(1)).rolling(30).std() * np.sqrt(365),
        'format' : '.2%'
    },
    {
        'label': 'Spread BRENT WTI ($)',
        'transform': lambda df: df['Spot BRENT'] - df['Spot WTI'],
        'format' : '+.2f'
    },
    {
        'label': 'Spot Natural Gas ($/MMBTU)',
        'transform': lambda df: df['Spot Natural Gas'],
        'format' : '.2f'
    },
    {
        'label': 'Spot Natural Gas Total Return',
        'transform': lambda df: np.log(df['Spot Natural Gas'].iloc[-1] / df['Spot Natural Gas']),
        'format' : '+.2%'
    },
]

today = raw_data.index[-1].to_pydatetime().date()
timeframes = [today, '1D', '5D', '1M', 'YTD', '1Y']

raw_df = call_pipeline(raw_data, raw_pip, timeframes, raw_data.index[-1])

raw_table = """<table style="font-family: Segoe UI, Tahoma, sans-serif; font-size: 14px;">
    <tr><td><strong>Spot BRENT</strong></td><td>: Cours quotidien de pétrole brut en Mer du Nord.</td></tr>
    <tr><td><strong>Spot WTI</strong></td><td>: Cours quotidien de pétrole brut aux Etats Unis.</td></tr>
    <tr><td><strong>Spot Natural Gas</strong></td><td>: Cours quotidien du Gas Naturel non liquéfié aux Etats Unis.</td></tr>
</table>"""

# %%
raw_df

# %% [markdown]
# ### CNN - Fear and Greed

# %%
import pandas as pd
from EcoWatch.Scraping import cnn

cnn_data = cnn()

cnn_pip = [
    {
        'label': 'Fear and Greed (0–100 scale)',
        'transform': lambda df: df['Fear and Greed'],
        'format' : '.2f'
    },
    {
        'label': 'Market Momentum S&P500 (pts)',
        'transform': lambda df: df['Market Momentum SP500'],
        'format' : ',.0f'
    },
    {
        'label': 'Stock Price Strength (%)',
        'transform': lambda df: df['Stock Price Strength'],
        'format' : '.2%'
    },
    {
        'label': 'Stock Price Breadth (pts)',
        'transform': lambda df: df['Stock Price Breadth'],
        'format' : ',.0f'
    },
    {
        'label': 'Put Call Options (%)',
        'transform': lambda df: df['Put Call Options'],
        'format' : '.2%'
    },
    {
        'label': 'Market Volatility VIX (0-100 scale)',
        'transform': lambda df: df['Market Volatility VIX'],
        'format' : '.2f'
    },
    {
        'label': 'Safe Haven Demand (%)',
        'transform': lambda df: df['Safe Haven Demand']/100,
        'format' : '+.2%'
    },
    {
        'label': 'Junk Bond Demand (%)',
        'transform': lambda df: df['Junk Bond Demand']/100,
        'format' : '+.2%'
    }
]

today = cnn_data.index[-1].to_pydatetime().date()
timeframes = [today, '1D', '5D', '1M', 'YTD']

cnn_df = call_pipeline(cnn_data, cnn_pip, timeframes, cnn_data.index[-1])

rating = cnn_data[[
    'Fear and Greed Rating',
    'Market Momentum SP500 Rating',
    'Stock Price Strength Rating',
    'Stock Price Breadth Rating',
    'Put Call Options Rating',
    'Market Volatility VIX Rating',
    'Junk Bond Demand Rating',
    'Safe Haven Demand Rating'
]].iloc[-1].to_list()

cnn_df['Actual Rating'] = rating
cnn_df = cnn_df[['Actual Rating'] + [tf for tf in timeframes]]

cnn_table = """<table style="font-family: Segoe UI, Tahoma, sans-serif; font-size: 14px;">
    <tr><td><strong>Indice Fear & Greed</strong></td><td>: Mesure le sentiment global des marchés en évaluant l’équilibre entre peur et avidité des investisseurs.</td></tr>
    <tr><td><strong>Market Momentum</strong></td><td>: Reflète la tendance haussière ou baissière du S&P 500 par rapport à sa moyenne mobile sur 125 jours.</td></tr>
    <tr><td><strong>Stock Price Strength</strong></td><td>: Evalue la proportion d’actions atteignant des sommets sur 52 semaines, un excès de nouveaux records indique un climat d’avidité.</td></tr>
    <tr><td><strong>Stock Price Breadth</strong></td><td>: Mesure le volume d’actions en hausse versus en baisse, une large participation positive traduit un signal de greed.</td></tr>
    <tr><td><strong>Put and Call Options</strong></td><td>: Reflète les anticipations des investisseurs, une hausse des put traduit une aversion au risque et traduit la peur.</td></tr>
    <tr><td><strong>VIX</strong></td><td>: Mesure la volatilité implicite du marché, augmente en période de tension et de baisse des marchés.</td></tr>
    <tr><td><strong>Safe Haven Demand</strong></td><td>: Compare les performances des obligations d’État et des actions, une surperformance obligataire indiquant une recherche de sécurité.</td></tr>
    <tr><td><strong>Junk Bond Demand</strong></td><td>: Indique l’appétit pour le risque : un resserrement des spreads est interprété comme un signal de greed.</td></tr>
</table>"""

# %%
cnn_df

# %% [markdown]
# ### Spreads

# %%
from EcoWatch.Scraping import oat, tbond, ester, bunds, fed_funds, euro_yield
import datetime as dt
import pandas as pd

ester_df = ester()
sofr_df = fed_funds('SOFR')['Rate (%)']

oat_df = oat()
bunds_df = bunds()
euro_df = euro_yield(category='ARI')
tbond_df = tbond('2023', str(dt.datetime.now().year))

bonds_data = pd.DataFrame({
    '10Y France'    : oat_df['10Y'],
    '10Y Allemagne' : bunds_df['10Y'],
    '10Y Europe'    : euro_df['10Y'],
    '10Y Etats Unis': tbond_df['10Y'],
    'ESTER'         : ester_df,
    'SOFR'          : sofr_df
})
bonds_data = bonds_data.dropna()
bonds_data = bonds_data / 100

# %%
bonds_pip = [
    {
        'label': '10Y France (yield)',
        'transform': lambda df: df['10Y France'],
        'format' : '+.2%'
    },
    {
        'label': '10Y Allemagne (yield)',
        'transform': lambda df: df['10Y Allemagne'],
        'format' : '+.2%'
    },
    {
        'label': '10Y Europe (yield)',
        'transform': lambda df: df['10Y Europe'],
        'format' : '+.2%'
    },
    {
        'label': '10Y Etats Unis (yield)',
        'transform': lambda df: df['10Y Etats Unis'],
        'format' : '+.2%'
    },
    {
        'label': '10Y France / ESTER (bp)',
        'transform': lambda df: df['10Y France'] / df['ESTER'] * 100,
        'format' : '.0f'
    },
    {
        'label': '10Y Allemagne / ESTER (bp)',
        'transform': lambda df: df['10Y Allemagne'] / df['ESTER'] * 100,
        'format' : '.0f'
    },
    {
        'label': '10Y Europe / ESTER (bp)',
        'transform': lambda df: df['10Y Europe'] / df['ESTER'] * 100,
        'format' : '.0f'
    },
    {
        'label': '10Y Etats Unis / SOFR (bp)',
        'transform': lambda df: df['10Y Etats Unis'] / df['SOFR'] * 100,
        'format' : '.0f'
    },
]

today = bonds_data.index[-1].to_pydatetime().date()
timeframes = [today, '1D', '5D', '1M', 'YTD', '1Y']

bonds_df = call_pipeline(bonds_data, bonds_pip, timeframes, bonds_data.index[-1])

# %%
bonds_df

# %% [markdown]
# ### Politique Monétaire US

# %%
from settings import FRED_KEY
from EcoWatch.Scraping import fred
import pandas as pd

usp_args = {
    '10Y Treasury Bond Yield'       : 'DGS10',
    'Federal Funds Effective Rate'  : 'FEDFUNDS',
    'Breakeven Inflation (10Y)'     : 'T10YIE',
    'Unemployment Rate'             : 'UNRATE',
    'Secured Overnight Fund Rate'   : 'SOFR',
    "Moody's US Aaa Corp. Bond"     : 'DAAA'
}

usp_data = {}
for series, args in usp_args.items():
    data = fred(key=FRED_KEY, ticker=args)
    data.index = pd.to_datetime(data.index).tz_localize(None)
    data = data.sort_index()
    data = data.resample('B').ffill()
    usp_data[series] = data

all_indices = [df.index for df in usp_data.values()]
start = max(idx.min() for idx in all_indices)
end = min(idx.max() for idx in all_indices)
full_index = pd.date_range(start=start, end=end, freq='B')

aligned_data = {
    name: df.reindex(full_index).ffill()
    for name, df in usp_data.items()
}

usp_data = pd.concat(aligned_data.values(), axis=1)
usp_data.columns = list(aligned_data.keys())


# %%
usp_pip = [
    {
        'label': 'Federal Funds Effective Rate',
        'transform': lambda df: df['Federal Funds Effective Rate']/100,
        'format' : '.2%'
    },
    {
        'label': 'Breakeven Inflation (10Y)',
        'transform': lambda df: df['Breakeven Inflation (10Y)']/100,
        'format' : '.2%'
    },
    {
        'label': 'Unemployment Rate',
        'transform': lambda df: df['Unemployment Rate']/100,
        'format' : '.2%'
    },   
    {
        'label': '10Y Treasury Bond Yield',
        'transform': lambda df: df['10Y Treasury Bond Yield']/100,
        'format' : '.2%'
    },
    {
        'label': 'Secured Overnight Fund Rate',
        'transform': lambda df: df['Secured Overnight Fund Rate']/100,
        'format' : '.2%'
    },
    {
        'label': 'Secured Overnight Fund Rate',
        'transform': lambda df: df['Secured Overnight Fund Rate']/100,
        'format' : '.2%'
    },
]

today = usp_data.index[-1].to_pydatetime().date()
timeframes = [today, '1M', '6M', 'YTD', '1Y']

usp_df = call_pipeline(usp_data, usp_pip, timeframes, usp_data.index[-1])

# %%
usp_df

# %% [markdown]
# ## 3. Partie Calendrier

# %%
import pandas as pd
import datetime as dt
from investpy import economic_calendar

from_date = dt.datetime.now().strftime('%d/%m/%Y')
to_date = (dt.datetime.now() + dt.timedelta(days=0)).strftime('%d/%m/%Y')

calendar_df = economic_calendar(
    countries = ['euro zone', 'united states', 'china', 'france'],
    importances = ['high', 'medium'],
    from_date = None,
    to_date = None
)

calendar_df.index = calendar_df['id']

for event in calendar_df.index:
    importance = calendar_df.loc[event, 'importance']
    zone = calendar_df.loc[event, 'zone']
    if importance == 'medium' and zone == 'united states':
        calendar_df.drop(event, axis=0, inplace=True)

country_list = calendar_df['zone']
country_list = [country.replace("china", 'Chine') for country in country_list]
country_list = [country.replace("france", 'France') for country in country_list]
country_list = [country.replace("united states", 'Etats-Unis') for country in country_list]
country_list = [country.replace("euro zone", 'Zone Euro') for country in country_list]
calendar_df = calendar_df.set_index(pd.Series(country_list, name='Pays'))

calendar_df = calendar_df.drop(columns=['id', 'date', 'currency', 'importance', 'zone'])
calendar_df.columns = ['Heure (UTC +2:00)', 'Evènement', 'Actuel', 'Consensus', 'Précédent']
calendar_df = calendar_df.fillna('-')

# %% [markdown]
# ## 4. Partie Revue d'Articles

# %% [markdown]
# ### Le Monde

# %%
from settings import MISTRAL_KEY
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from mistralai import Mistral

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
lm_url = ['https://www.lemonde.fr/politique/']
mistral_model = "mistral-medium-2505"
key = MISTRAL_KEY
lm_article = []

for url in lm_url:
    content = f""
    articles = []
    r = requests.get(url=url, headers=headers)
    soup = BeautifulSoup(r.content, 'html.parser')
    spans = soup.find_all('h3', class_='teaser__title')
    for span in soup.find_all('a', class_='teaser__link'):
        title = span.get_text(strip=True)
        full_url = urljoin(url, span.get("href"))
        articles.append(
            {
                "title": title,
                "url": full_url
            }
        )

    content = f"""
    Tu es un assistant spécialisé en actualité financière

    Ta tâche : extraire les 5 informations les plus importantes à partir de l’ensemble des articles que je te mets à disposition

    Contraintes :
    - La liste des articles est fournie dans la variable `articles` sous forme de liste de dictionnaires
    - La sélection d'articles que tu dois renvoyer doit prendre la forme d'un code html et **suivre exactement le format attendu**
    - Ne commencer ni par une introduction ni par une explication, uniquement par la liste numérotée
    - Ne pas inclure de balises HTML, de liens hypertextes ou de citations
    - Employer un ton neutre, synthétique et professionnel
    - Chaque point doit être formulé en **français**, en **une seule phrase**, **courte**, claire et autonome
    - Commencer chaque point par l’indication du pays concerné sous forme du **code alpha-2** du pays concerné par l'article (US, FR, UK, etc.)
    - Terminer chaque point par le lien direct vers l’article spécifique utilisé pour justifier l’information, entre parenthèses
    - Le lien doit obligatoirement être celui de l’article utilisé : ne jamais donner de lien vers une page d’accueil, un flux RSS ou une rubrique générale
    - Ne jamais débuter un point par “selon”, “d’après”, “un article indique que”, etc
    - Ne pas inclure de conclusion ni de phrase récapitulative
    - Si moins de 5 informations pertinentes sont disponibles dans une catégorie, n’en retourner que le nombre exact
    - Ne rien inventer pour compléter artificiellement la liste
    - Attention à ne pas modifier les liens de articles, ne pas ajouter de point (.) à la fin des liens

    Format attendu :
    <ol>
        <li>code alpha-2 : résumé de l’information - <a href=url>Lire l'article</a></li>
        <li>code alpha-2 : résumé de l’information - <a href=url>Lire l'article</a></li>
        ...
    </ol>

    Articles à analyser : 
    {articles}
    """

    client = Mistral(api_key=key)
    chat_response = client.chat.complete(
        model= mistral_model,
        messages = [
            {
                "role": "user",
                "content": content,
            },
        ]
    )
    lm_article.append(chat_response.choices[0].message.content)
lm_article = [article.replace('*', '') for article in lm_article]
lm_article = [article.replace('```', '') for article in lm_article]
lm_article = [article.replace('html', '') for article in lm_article]
lm_article = [article.replace('.>', '>') for article in lm_article]

# %% [markdown]
# ### Financial Times

# %%
from settings import MISTRAL_KEY
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from mistralai import Mistral

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
ft_url = ['https://www.ft.com/world', 'https://www.ft.com/markets']
mistral_model = "mistral-medium-2505"
key = MISTRAL_KEY
ft_article = []

for url in ft_url:
    content = f""
    articles = []
    r = requests.get(url=url, headers=headers)
    soup = BeautifulSoup(r.content, 'html.parser')
    spans1 = soup.find_all('a', class_='js-teaser-standfirst-link')
    spans2 = soup.find_all('a', class_='js-teaser-heading-link')
    for span1, span2 in zip(spans2, spans1):
        title = span1.get_text(strip=True)
        subtitle = span2.get_text(strip=True)
        full_url = urljoin(url, span1.get("href"))
        articles.append(
            {
                "title": title,
                "subtitle": subtitle,
                "url": full_url
            }
        )

    content = f"""
    Tu es un assistant spécialisé en actualité financière

    Ta tâche : extraire les 5 informations les plus importantes à partir de l’ensemble des articles que je te mets à disposition

    Contraintes :
    - La liste des articles est fournie dans la variable `articles` sous forme de liste de dictionnaires
    - La sélection d'articles que tu dois renvoyer doit prendre la forme d'un code html et **suivre exactement le format attendu**
    - Ne commencer ni par une introduction ni par une explication, uniquement par la liste numérotée
    - Ne pas inclure de balises HTML, de liens hypertextes ou de citations
    - Employer un ton neutre, synthétique et professionnel
    - Chaque point doit être formulé en **français**, en **une seule phrase**, **courte**, claire et autonome
    - Commencer chaque point par l’indication du pays concerné sous forme du **code alpha-2** du pays concerné par l'article (US, FR, UK, etc.)
    - Terminer chaque point par le lien direct vers l’article spécifique utilisé pour justifier l’information, entre parenthèses
    - Le lien doit obligatoirement être celui de l’article utilisé : ne jamais donner de lien vers une page d’accueil, un flux RSS ou une rubrique générale
    - Ne jamais débuter un point par “selon”, “d’après”, “un article indique que”, etc
    - Ne pas inclure de conclusion ni de phrase récapitulative
    - Si moins de 5 informations pertinentes sont disponibles dans une catégorie, n’en retourner que le nombre exact
    - Ne rien inventer pour compléter artificiellement la liste

    Format attendu :
    <ol>
        <li>code alpha-2 : résumé de l’information - <a href=url>Lire l'article</a></li>
        <li>code alpha-2 : résumé de l’information - <a href=url>Lire l'article</a></li>
        ...
    </ol>

    Articles à analyser : 
    {articles}
    """

    client = Mistral(api_key=key)
    chat_response = client.chat.complete(
        model= mistral_model,
        messages = [
            {
                "role": "user",
                "content": content,
            },
        ]
    )
    ft_article.append(chat_response.choices[0].message.content)
ft_article = [article.replace('*', '') for article in ft_article]
ft_article = [article.replace('```', '') for article in ft_article]
ft_article = [article.replace('html', '') for article in ft_article]

# %% [markdown]
# ## 5. Ecriture et envoi du mail

# %%
from settings import MAIL_PASSWORD
import smtplib
import ssl
from email.message import EmailMessage
import winsound
import json

with open('metadata.json', 'r') as f:
    metadata = json.load(f)
    receiver = metadata['receiver']
    sender = metadata['sender']

subject = f'Macro Brief - {dt.datetime.now().strftime("%d-%m-%Y")}'
body = f"""
<html>
    <body>
        <style>
        body {{font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;}}
    </style>
        <h1>Macro Brief</h1>
        <p>{synthesis}</p>
        <h2>Météo Parisienne</h2>
        {format_html(weather_df)}
        <h2>Revue de Marchés</h2>
        <h3>Actions :</h3>
        {format_html(market_df)}
        <h3>CNN Fear and Greed :</h3>
        {format_html(cnn_df)}
        <p>{cnn_table}</p>
        <h3>Devises :</h3>
        {format_html(currency_df)}
        <h3>Obligations :</h3>
        {format_html(bonds_df)}
        <h3>Alternatifs :</h3>
        {format_html(alter_df)}
        <p>{alter_table}</p>
        <h3>Crypto Actifs :</h3>
        {format_html(crypto_df)}
        <h3>Matières Premières :</h3>
        {format_html(raw_df)}
        <p>{raw_table}</p>
        <h2>Calendrier</h2>
        {format_html(calendar_df)}
        <h2>Politiques Monétaires</h2>
        <h3>US Federal Reserve :</h3>
        {format_html(usp_df)}
        <h3>Banque Centrale Européenne :</h3>
        <p><em>En construction</em></p>      
        <h2>Revue de Presse</h2>
        <h3>Politique Européenne:</h3>
        <p>{lm_article[0]}</p>
        <h3>Géopolitique:</h3>
        <p>{ft_article[0]}</p>
        <h3>Finance:</h3>
        <p>{ft_article[1]}</p>
        <h2>Sources de Rapport:</h2>
        <p>{source}</p>
    </body>
</html>
"""

em = EmailMessage()
em['From'] = sender
em['To'] = sender
em['Subject'] = subject
em.set_content(body, subtype='html')
em['Bcc'] = ", ".join(receiver)

context = ssl.create_default_context()
with smtplib.SMTP_SSL('smtp.gmail.com', 465, context=context) as smtp:
    smtp.login(sender, MAIL_PASSWORD)
    smtp.send_message(em)

winsound.MessageBeep(winsound.MB_OK)

# %%



