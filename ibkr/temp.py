import os
from pathlib import Path
print(os.path.dirname(__file__))
print(os.path.join(os.path.dirname(__file__), Path('IBKR_TICKERS.json')))
print(Path(__file__))
print(Path(__file__).resolve().parent / 'IBKR_TICKERS.json')