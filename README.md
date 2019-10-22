# Databricks Admin Center (DAC)

Databricks Admin Center (DAC) solves the problem of overview of Databricks workspaces, clusters and Users.

# Running
0) Copy the githooks from `.githooks` to `.git/hooks`

1) Create and activate virtualenv environment
```
virtualenv --no-site-packages --python=python3.6 .venv
source .venv/bin/activate
```

2) Install requirements
```
pip install -r requirements.txt
```

3) Source the `.env` file to load the environmental variables. 
```
source .env
```
If you don't have the .env, create it::
```
export DATABRICKS_TOKEN_MAIN_AWS=**********
export DATABRICKS_TOKEN_MAIN_AZURE==**********
export DATABRICKS_TOKEN_MAIN_LIDL==**********
export FLASK_APP=main.py
export FLASK_ENV=development
```

4) Create the db and start the scraper:
```
rm -f dac.db && python main.py create_db && python main.py scrape
```

5) Start the server:
```
python main.py ui
```

# Technology
- Python 3+
- Flask
- SQLAlchemy
- Chart.js
- databricks-api
