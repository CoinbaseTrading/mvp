# Coinbase data load

This repo contains the code used to load data from Coinbase into our Postgres database.

# Local setup

Before you get started, you need to setup the following variables in your local environment:
```
PG_HOST
PG_DATABASE
PG_USER
PG_PASSWORD
SLACK_BOT_TOKEN
SLACK_CHANNEL
```

# Running locally with docker

To load candles from Coinbase Pro to the database, run:
```shell
docker-compose up --build --remove-orphans load_candles
```
To adjust the "days to load", update the docker-compose.yaml file.

# Unit testing

To run unit tests, you can use:
```shell
make test
```
