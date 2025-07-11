import os
from os.path import join, dirname
from dotenv import load_dotenv

#echo openweather_key='API_key' >.env

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path)

OPENWEATHER_KEY = os.environ.get("OPENWEATHER_KEY")
FRED_KEY = os.environ.get('FRED_KEY')
MISTRAL_KEY = os.environ.get('MISTRAL_KEY')
MAIL_PASSWORD = os.environ.get('MAIL_PASSWORD')
TRADINGECONOMICS_KEY = os.environ.get('TRADINGECONOMICS_KEY') # Pas utilisé car version complète payante --> seulement accès à quelques pays
EIA_KEY = os.environ.get('EIA_KEY') # US Energy Information Administration
BINANCE_KEY = os.environ.get('BINANCE_KEY')
BINANCE_SECRET = os.environ.get('BINANCE_SECRET')