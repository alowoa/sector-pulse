# SectorPulse

> Automated weekly sector sentiment playbook for US equity rotation — powered by FinBERT and financial news NLP.

## Requirements

* Python 3.12+

## Quickstart

```bash
# sync venv + dependencies
uv sync 

# Copy & fill API keys in .env
cp .env.example .env 

# run
uv run src/main/python/main.py

```

## Technical stack

SectorPulse is built with:
* [FastAPI](https://fastapi.tiangolo.com/)
* [FinBERT](https://github.com/finbert-ai/finbert)

## Disclaimer

SectorPulse is built for educational purposes. The Sector Playbook does not give financial advice. 

Tested on Mac OS only.