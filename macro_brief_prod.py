# %% [markdown]
# ## 1. Introduction et Conclusion

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
from bs4 import BeautifulSoup
import requests

def le_monde(url):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    r = requests.get(url=url, headers=headers)
    soup = BeautifulSoup(r.content, 'html.parser')

    article = []
    threads = soup.find_all('div', 'thread')
    for thread in threads:
        metadata = {}
        title = thread.find('h3', class_='teaser__title').text
        desc = thread.find('p', class_='teaser__desc').text
        metadata['title'] = title + ' : ' + desc
        metadata['date'] = thread.find('span', class_='meta__date').text
        metadata['link'] = thread.find('a', class_='teaser__link')['href']
        article.append(metadata)
    return article

# %%
import pandas as pd
import datetime as dt
import time
import random
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import undetected_chromedriver as uc

def reuters(url):
    #Initialize driver with undetected_chromedriver
    options = uc.ChromeOptions()
    #options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-infobars")
    driver = uc.Chrome(options=options, headless=False)
    driver.get(url)
    wait = WebDriverWait(driver, 10)

    # Reject Cookies
    time.sleep(random.uniform(1.2, 1.8))
    try:
        wait.until(
            EC.element_to_be_clickable(
                (By.ID, "onetrust-reject-all-handler")
            )
        ).click()
    except:
        pass

    # Load more articles
    try:
        for _ in range(random.randint(2, 3)):
            for _ in range(random.randint(3, 5)):
                    scroll_amount = random.randint(150, 250)
                    driver.execute_script(f"window.scrollBy(0, {scroll_amount});")
                    time.sleep(random.uniform(0.9, 1.5))
            wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "//span[@data-testid='Text' and contains(text(), 'Load more articles')]")
                )
            ).click()
            time.sleep(random.uniform(1.2, 1.8))
    except:
        pass

    # Fetch articles
    module = driver.find_elements(By.CLASS_NAME, "media-story-card-module__body__nZjo1")
    exclusions = ['\n', 'ANALYSIS', 'category']
    article = []
    for mod in module:
        children = mod.find_elements(By.XPATH, ".//*")
        data = []
        for child in children:
            data.append(child.text)
            raw = child.get_attribute("href")
            if raw and len(raw) > 50: link = child.get_attribute("href")
        data = [item for item in data if not any(excl in item for excl in exclusions)]
        data = list(dict.fromkeys(data))
        article.append({'title':data[0], 'date':data[1], 'link':link})

    driver.quit()
    return article
     

# %%
from mistralai import Mistral

def mistral(key, article_list, subject='les marchés', len_output=5):
    mistral_model = "mistral-medium-2505"

    content = f"""
    Tu es un assistant spécialisé en actualité financière.

    Ta tâche : extraire les {len_output} informations les plus importantes à partir de la liste de metadata d'articles.

    Contraintes :
    - Les informations doivent être en lien avec : **{subject}**
    - L'importance des informations est basée sur deux points : l'impact de l'information sur les marchés, l'ancienneté de l'information
    - Le titre de l'article est fournie dans la variable `title`, le jour de publication de l'article est fourni dans la variable `date` et le lien de l'article est donné dans la variable `link`.
    - La variable `date` doit être renvoyé au format "dd/mm/yyyy"
    - La variable `symbol` correspond à l'alpha code 2 (e.g. FR, US, DE...) du pays concerné par l'information et n'est pas donnée, tu dois la déduire d'après le contexte de la variable `title`
    - La sélection d'articles que tu dois renvoyer doit prendre la forme d'un code html et **suivre exactement le format attendu**.
    - Ne commencer ni par une introduction ni par une explication, uniquement par la liste html.
    - Employer un ton neutre, synthétique et professionnel.
    - Chaque point doit être formulé en **français**, en **une seule phrase**, **courte**, claire et autonome.
    - Essayer de proposer **au moins un article par `symbol`** et prioriser les articles les plus récent en te basant sur la variable `time`.
    - Sélectionner uniquement les articles dont le titre est précis et donne assez d'informations sur le contenu de l'article.
    - Le lien `link` doit obligatoirement être celui de l’article utilisé : ne jamais donner de lien vers une page d’accueil, un flux RSS ou une rubrique générale.
    - Ne jamais débuter un point par “selon”, “d’après”, “un article indique que”, etc.
    - Ne pas inclure de conclusion ni de phrase récapitulative.
    - Si moins de {len_output} informations pertinentes sont disponibles dans une catégorie, n’en retourner que le nombre exact.
    - Ne rien inventer pour compléter artificiellement la liste.

    Format attendu :
    <table style="font-family: Segoe UI, Tahoma, sans-serif; font-size: 16px;">
        <tr><td><strong>`symbol` [`date`]</strong></td><td> « résumé de l’information à partir de `title` » <a href=`link`>Lire l'article</a></td></tr>
        <tr><td><strong>`symbol` [`date`]</strong></td><td> « résumé de l’information à partir de `title` » <a href=`link`>Lire l'article</a></td></tr>
        ...
    </table>

    Articles à analyser : 
    {article_list}
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
    article_review = (chat_response.choices[0].message.content)
    article_review = article_review.replace('*', '')
    article_review = article_review.replace('```', '')
    article_review = article_review.replace('html', '')
    return article_review

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
    'CAC 40'            : {'ticker':'^FCHI', 'value':'Close'},
    'S&P 500'           : {'ticker':'^GSPC', 'value':'Close'},
    'NASADQ 100'        : {'ticker':'^IXIC', 'value':'Close'},
    'Nikkei 225'        : {'ticker':'^N225', 'value':'Close'},
    'Hang Seng Index'   : {'ticker':'^HSI', 'value':'Close'}
}

market_data = pd.DataFrame()
for symbol, args in market_args.items():
    data = yf.Ticker(args['ticker']).history(period='5y')
    data.index = pd.to_datetime(data.index)
    data.index = data.index.tz_localize(None)    
    for arg in args['value']:
        market_data[f"{symbol} {args['value']}"] = data[args['value']]

market_data = market_data.dropna()

market_pip = [
    {
        'label': 'CAC 40',
        'transform': lambda df: df['CAC 40 Close'],
        'format' : ',.2f'
    },
    {
        'label': 'CAC 40 Total Return',
        'transform': lambda df: np.log(df['CAC 40 Close'].iloc[-1] / df['CAC 40 Close']),
        'format' : '+.2%'
    },
    {
        'label': 'S&P 500',
        'transform': lambda df: df['S&P 500 Close'],
        'format' : ',.2f'
    },
    {
        'label': 'S&P 500 Total Return',
        'transform': lambda df: np.log(df['S&P 500 Close'].iloc[-1] / df['S&P 500 Close']),
        'format' : '+.2%'
    },
    {
        'label': 'NASDAQ 100',
        'transform': lambda df: df['NASADQ 100 Close'],
        'format' : ',.2f'
    },
    {
        'label': 'NASDAQ 100 Total Return',
        'transform': lambda df: np.log(df['NASADQ 100 Close'].iloc[-1] / df['NASADQ 100 Close']),
        'format' : '+.2%'
    },
    {
        'label': 'NIKKEI 225',
        'transform': lambda df: df['Nikkei 225 Close'],
        'format' : ',.2f'
    },
    {
        'label': 'NIKKEI 225 Total Return',
        'transform': lambda df: np.log(df['Nikkei 225 Close'].iloc[-1] / df['Nikkei 225 Close']),
        'format' : '+.2%'
    },
    {
        'label': 'Hang Seng Index',
        'transform': lambda df: df['Hang Seng Index Close'],
        'format' : ',.2f'
    },
    {
        'label': 'HSI Total Return',
        'transform': lambda df: np.log(df['Hang Seng Index Close'].iloc[-1] / df['Hang Seng Index Close']),
        'format' : '+.2%'
    },
]

today = market_data.index[-1].to_pydatetime().date()
timeframes = [today, '1D', '5D', '1M', 'YTD', '1Y']

market_df = call_pipeline(market_data, market_pip, timeframes, today=market_data.index[-1])

print(market_df.to_string())

# %%
from settings import MISTRAL_KEY

market_article = reuters('https://www.reuters.com/markets/us/')
market_review = mistral(
    MISTRAL_KEY, 
    market_article, 
    subject='les marchés actions', 
    len_output=3
)

print(market_review)

# %% [markdown]
# ### Devises

# %%
import yfinance as yf
import pandas as pd
import numpy as np

currency_args = {
    'EUR-USD'   : {'ticker':'^FCHI', 'value':'Close'},
    'GBP-USD'   : {'ticker':'^GSPC', 'value':'Close'},
    'USD-JPY'   : {'ticker':'^IXIC', 'value':'Close'},
    'USD-CNY'   : {'ticker':'^N225', 'value':'Close'},
    'USD-CHF'   : {'ticker':'^HSI', 'value':'Close'}
}

currency_data = pd.DataFrame()
for symbol, args in currency_args.items():
    data = yf.Ticker(args['ticker']).history(period='5y')
    data.index = pd.to_datetime(data.index)
    data.index = data.index.tz_localize(None)    
    for arg in args['value']:
        currency_data[f"{symbol} {args['value']}"] = data[args['value']]

currency_data = currency_data.dropna()

currency_pip = [
    {
        'label': 'EUR-USD',
        'transform': lambda df: df['EUR-USD Close'],
        'format' : ',.4f'
    },
    {
        'label': 'EUR-USD Total Return',
        'transform': lambda df: np.log(df['EUR-USD Close'].iloc[-1] / df['EUR-USD Close']),
        'format' : '+.2%'
    },
    {
        'label': 'EUR-USD Volatility',
        'transform': lambda df: np.log(df['EUR-USD Close'] / df['EUR-USD Close'].shift(1)).rolling(30).std() * np.sqrt(365),
        'format': '.2%'
    },
    {
        'label': 'GBP-USD',
        'transform': lambda df: df['GBP-USD Close'],
        'format' : ',.2f'
    },
    {
        'label': 'USD-JPY',
        'transform': lambda df: df['USD-JPY Close'],
        'format' : ',.0f'
    },
    {
        'label': 'USD-CNY',
        'transform': lambda df: df['USD-CNY Close'],
        'format' : ',.2f'
    },
    {
        'label': 'USD-CHF',
        'transform': lambda df: df['USD-CHF Close'],
        'format' : ',.2f'
    },
]

today = currency_data.index[-1].to_pydatetime().date()
timeframes = [today, '1D', '5D', '1M', 'YTD', '1Y']

currency_df = call_pipeline(currency_data, currency_pip, timeframes, today=currency_data.index[-1])

print(currency_df.to_string())

# %%
from settings import MISTRAL_KEY

currency_article = reuters('https://www.reuters.com/markets/currencies/')
currency_review = mistral(
    MISTRAL_KEY, 
    currency_article, 
    subject='le marché des devises', 
    len_output=3
)

print(currency_review)

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

print(crypto_df.to_string())

# %%
from settings import MISTRAL_KEY

#crypto_article = reuters('https://www.reuters.com/markets/cryptocurrency/')
#crypto_review = mistral(
#    MISTRAL_KEY, 
#    crypto_article, 
#    subject='le marché des crypto actifs', 
#    len_output=3
#)
#
#print(crypto_review)

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

print(alter_df.to_string())

# %%
alter_table = """<table style="font-family: Segoe UI, Tahoma, sans-serif; font-size: 14px; font-weight: normal;">
    <tr><td><strong>Emerging Markets</strong></td><td>: Indice MSCI Emerging Markets (Proxi : iShares Core MSCI Emerging Markets ETF).</td></tr>
    <tr><td><strong>Real Estate</strong></td><td>: Indice FTSE EPRA/NAREIT Developed (Proxi : HSBC FTSE EPRA NAREIT Developed UCITS ETF).</td></tr>
    <tr><td><strong>Private Equity</strong></td><td>: Indice Red Rocks Global Listed Private Equity Index (Proxi : Invesco Global Listed Private Equity ETF).</td></tr>
    <tr><td><strong>Infrastructure</strong></td><td>: Indice S&P Global Infrastructure Index (Proxi : iShares Global Infrastructure ETF).</td></tr>
</table>"""

print(alter_table)

# %%
from settings import MISTRAL_KEY

alter_article = reuters('https://www.reuters.com/markets/funds/')
alter_review = mistral(
    MISTRAL_KEY, 
    alter_article, 
    subject="les marchés de l'immobilier, " \
    "du private equity, " \
    "des hedgefunds, " \
    "de l'or, " \
    "de l'infrastructure, " \
    "de la dette privée", 
    len_output=3
)

print(alter_review)

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

print(raw_df.to_string())

# %%
raw_table = """<table style="font-family: Segoe UI, Tahoma, sans-serif; font-size: 14px; font-weight: normal;">
    <tr><td><strong>Spot BRENT</strong></td><td>: Cours quotidien de pétrole brut en Mer du Nord.</td></tr>
    <tr><td><strong>Spot WTI</strong></td><td>: Cours quotidien de pétrole brut aux Etats Unis.</td></tr>
    <tr><td><strong>Spot Natural Gas</strong></td><td>: Cours quotidien du Gas Naturel non liquéfié aux Etats Unis.</td></tr>
</table>"""

print(raw_table)

# %%
from settings import MISTRAL_KEY

raw_article = reuters('https://www.reuters.com/markets/commodities/')
raw_review = mistral(
    MISTRAL_KEY, 
    raw_article, 
    subject="les matières premières et notamment le pétrole et le gas naturel", 
    len_output=3
)

print(raw_review)

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

print(cnn_df.to_string())

# %%
cnn_table = """<table style="font-family: Segoe UI, Tahoma, sans-serif; font-size: 14px; font-weight: normal;">
    <tr><td><strong>Indice Fear & Greed</strong></td><td>: Mesure le sentiment global des marchés en évaluant l’équilibre entre peur et avidité des investisseurs.</td></tr>
    <tr><td><strong>Market Momentum</strong></td><td>: Reflète la tendance haussière ou baissière du S&P 500 par rapport à sa moyenne mobile sur 125 jours.</td></tr>
    <tr><td><strong>Stock Price Strength</strong></td><td>: Evalue la proportion d’actions atteignant des sommets sur 52 semaines, un excès de nouveaux records indique un climat d’avidité.</td></tr>
    <tr><td><strong>Stock Price Breadth</strong></td><td>: Mesure le volume d’actions en hausse versus en baisse, une large participation positive traduit un signal de greed.</td></tr>
    <tr><td><strong>Put and Call Options</strong></td><td>: Reflète les anticipations des investisseurs, une hausse des put traduit une aversion au risque et traduit la peur.</td></tr>
    <tr><td><strong>VIX</strong></td><td>: Mesure la volatilité implicite du marché, augmente en période de tension et de baisse des marchés.</td></tr>
    <tr><td><strong>Safe Haven Demand</strong></td><td>: Compare les performances des obligations d’État et des actions, une surperformance obligataire indiquant une recherche de sécurité.</td></tr>
    <tr><td><strong>Junk Bond Demand</strong></td><td>: Indique l’appétit pour le risque : un resserrement des spreads est interprété comme un signal de greed.</td></tr>
</table>"""

# %% [markdown]
# ### Obligations

# %%
from EcoWatch.Scraping import oat, tbond, bunds, fed_funds, ecb
import datetime as dt
import pandas as pd

ester_df = ecb('EST.B.EU000A2QQF16.CR')
sofr_df = fed_funds('SOFR')['Rate (%)']

oat_df = oat()
bunds_df = bunds()
euro_df = ecb('YC.B.U2.EUR.4F.G_N_C.SV_C_YM.SR_10Y')
tbond_df = tbond('2023', str(dt.datetime.now().year))

bonds_data = pd.DataFrame({
    '10Y France'    : oat_df['10Y'],
    '10Y Allemagne' : bunds_df['10Y'],
    '10Y Europe'    : euro_df,
    '10Y Etats Unis': tbond_df['10Y'],
    'ESTER'         : ester_df,
    'SOFR'          : sofr_df
})
bonds_data = bonds_data.dropna()
bonds_data = bonds_data / 100

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

print(bonds_df.to_string())

# %%
from settings import MISTRAL_KEY

bonds_article = reuters('https://www.reuters.com/markets/rates-bonds/')
bonds_review = mistral(
    MISTRAL_KEY, 
    bonds_article, 
    subject="les marchés obligataires américains et européens", 
    len_output=3
)

print(bonds_review)

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

usp_pip = [
    {
        'label': 'FFR',
        'transform': lambda df: df['Federal Funds Effective Rate']/100,
        'format' : '.2%'
    },
    {
        'label': 'SOFR',
        'transform': lambda df: df['Secured Overnight Fund Rate']/100,
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
        'label': '10Y TBonds',
        'transform': lambda df: df['10Y Treasury Bond Yield']/100,
        'format' : '.2%'
    },
    {
        'label': "US AAA Corp. Bonds",
        'transform': lambda df: df["Moody's US Aaa Corp. Bond"]/100,
        'format' : '.2%'
    },
]

today = usp_data.index[-1].to_pydatetime().date()
timeframes = [today, '1M', '6M', 'YTD', '1Y']

usp_df = call_pipeline(usp_data, usp_pip, timeframes, usp_data.index[-1])

print(usp_df.to_string())

# %%
###### Rajouter la définition des champs

# %% [markdown]
# ### Politique Monétaire EU

# %%
from EcoWatch.Scraping import ecb
import pandas as pd
today = pd.Timestamp.today().normalize()
full_index = pd.date_range(start=real_yield.index.min(), end=today, freq='B')

mro = ecb('FM.D.U2.EUR.4F.KR.MRR_FR.LEV')
unploy = ecb('LFSI.M.I9.S.UNEHRT.TOTAL0.15_74.T')
inflation = ecb(' ICP.M.U2.N.000000.4.ANR')
state_yield = ecb('YC.B.U2.EUR.4F.G_N_C.SV_C_YM.SR_10Y')
ester = ecb('EST.B.EU000A2QQF16.CR')
corp_rate = ecb('MIR.M.U2.B.A2I.AM.R.A.2240.EUR.N')
corp_rate = corp_rate.reindex(full_index).ffill()

par_yield = ecb('YC.B.U2.EUR.4F.G_N_C.SV_C_YM.PY_10Y')
real_yield = ecb('FM.M.U2.EUR.4F.BB.R_U2_10Y.YLDA')
real_yield = real_yield.reindex(full_index).ffill()

eup_data = pd.DataFrame({
    'Main Refinancing Operations' : mro,
    'Breakeven Inflation (10Y)' : par_yield - real_yield,
    'Unemployment Rate' : unploy,
    '10Y Euro Gvt. Bonds' : state_yield,
    "10Y Euro Corp. Bond" : corp_rate,
    'ESTR' : ester,
})

eup_pip = [
    {
        'label': 'MRO',
        'transform': lambda df: df['Main Refinancing Operations']/100,
        'format' : '.2%'
    },
    {
        'label': "€STR",
        'transform': lambda df: df["ESTR"]/100,
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
        'label': '10Y Euro Gvt. Bonds',
        'transform': lambda df: df['10Y Euro Gvt. Bonds']/100,
        'format' : '.2%'
    },
    {
        'label': '10Y Euro Corp. Bond',
        'transform': lambda df: df['10Y Euro Corp. Bond']/100,
        'format' : '.2%'
    }
]

today = eup_data.index[-1].to_pydatetime().date()
timeframes = [today, '1M', '6M', 'YTD', '1Y']

eup_df = call_pipeline(eup_data, eup_pip, timeframes, usp_data.index[-1])

print(eup_df.to_string())

# %%
eup_table = """<table style="font-family: Segoe UI, Tahoma, sans-serif; font-size: 14px; font-weight: normal;">
    <tr><td><strong>Main Refinancing Operations (MRO)</strong></td><td>: taux d’intérêt auquel les banques empruntent des liquidités à court terme auprès de la BCE contre une garantie éligible.</td></tr>
    <tr><td><strong>Unemployment Rate</strong></td><td>: Taux moyen de chômage constaté dans la zone euro (20 membres), non harmonisé.</td></tr>
    <tr><td><strong>Harmonised Index of Consumer Prices (HICP)</strong></td><td>: Taux d’inflation mesuré par l’indice des prix à la consommation harmonisé en zone euro</td></tr>
    <tr><td><strong>Breakeven Inflation (10Y)</strong></td><td>: Différence entre le taux nominal et le taux réel, représente l'inflation moyenne anticipée par le marché sur cette période</td></tr>
    <tr><td><strong>10Y Euro Bonds</strong></td><td>: Taux d'intérêt moyen des obligations d'état zero coupon à 10 ans des pays de la zone euro</td></tr>
    <tr><td><strong>Euro Short Term Rate (€STR)</strong></td><td>: Taux de référence de l'euro au jour le jour, calculé par la BCE sur la base des taux du marché monétaire en euro</td></tr>
</table>"""

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

if calendar_df.empty:
    pass
else:
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

print(calendar_df.to_string())

# %% [markdown]
# ## 4. Partie Revue d'Articles

# %% [markdown]
# ### Le Monde

# %%
from settings import MISTRAL_KEY

# International
international_article = le_monde('https://www.lemonde.fr/international/')

international_review = mistral(
    MISTRAL_KEY,
    international_article,
    subject="l'actualité géopolitique internationale",
    len_output=5
)

print(international_review)

# Economie
economic_article = le_monde('https://www.lemonde.fr/economie-mondiale/')

economic_review = mistral(
    MISTRAL_KEY,
    economic_article,
    subject="l'économie mondiale",
    len_output=5
)

print(economic_review)

# Politique
politic_article = le_monde('https://www.lemonde.fr/politique/')

politic_review = mistral(
    MISTRAL_KEY,
    politic_article,
    subject="la politique Européenne et Française",
    len_output=5
)

print(politic_review)

# %% [markdown]
# ## 5. Ecriture et envoi du mail

# %%
from settings import MAIL_PASSWORD
import smtplib
import ssl
from email.message import EmailMessage
import winsound
import json
import os

with open(os.path.join(os.path.dirname(__file__), 'metadata_test.json'), 'r') as f:
#with open(os.getcwd() + '\\metadata_test.json', 'r') as f:
    metadata = json.load(f)
    receiver = metadata['receiver']
    sender = metadata['sender']

subject = f'Macro Brief - {dt.datetime.now().strftime("%d-%m-%Y")}'
body = f"""
<html>
    <body>
        <style>
        body {{font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;}}
        h1 {{font-size: 26px}}
        h2 {{font-size: 20px}}
        h3 {{font-size: 16px}}
        h2 + li {{font-size: 20px;font-weight: bold;}}
        h3 + li {{font-size: 16px;font-weight: bold;}}
    </style>
        <h1>Macro Brief</h1>
        <p>{synthesis}</p>
        <ol type="I">
            <li><h2>Météo Parisienne</h2></li>
            {format_html(weather_df)}
            <li><h2>Revue de Presse</h2></li>
            <ol type="1">
                <li><h3>Politique Européenne:</h3></li>
                <p>{politic_review}</p>
                <li><h3>Géopolitique</h3></li>
                <p>{international_review}</p>
                <li><h3>Finance</h3></li>
                <p>{economic_review}</p>
            </ol>
            <li><h2>Revue de Marchés</h2></li>
            <ol type="1">
                <li><h3>Actions</h3></li>
                <p>{market_review}</p>
                {format_html(market_df)}
                <li><h3>CNN Fear and Greed</h3></li>
                <p><em>Commentaire automatisé sur le Fear & Greed en construction</em></p>
                {format_html(cnn_df)}
                <p>{cnn_table}</p>
                <li><h3>Devises</h3></li>
                <p>{currency_review}</p>
                {format_html(currency_df)}
                <li><h3>Obligations</h3></li>
                <p>{bonds_review}</p>
                {format_html(bonds_df)}
                <li><h3>Alternatifs</h3></li>
                <p>{alter_review}</p>
                {format_html(alter_df)}
                <p>{alter_table}</p>
                <li><h3>Crypto Actifs</h3></li>
                <p><em>Revue d'article sur les Crypto Actifs en construction</em></p>
                {format_html(crypto_df)}
                <li><h3>Matières Premières</h3></li>
                <p>{raw_review}</p>
                {format_html(raw_df)}
                <p>{raw_table}</p>
            </ol>
            <li><h2>Calendrier</h2>
            {"<p>Pas d'évènement économique aujourd'hui</p>" if calendar_df.empty else format_html(calendar_df)}
            <li><h2>Politiques Monétaires</h2>
            <ol type="1">
                <li><h3>US Federal Reserve :</h3></li>
                {format_html(usp_df)}
                <p>rajouter table US ICI</p>
                <li><h3>Banque Centrale Européenne :</h3></li>
                {format_html(eup_df)}  
                <p>{eup_table}</p>
            </ol>
        </ol> 
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



