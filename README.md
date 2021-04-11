# Candles

This repo contains the code to load candles data from Coinbase Pro into our Postgres database.

# Local setup

Before you get started, you need to setup the following variables in your local environment:
```
PG_HOST
PG_DATABASE
PG_USER
PG_PASSWORD
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
