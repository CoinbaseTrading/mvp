from datetime import timedelta
import os

import numpy as np
import pandas as pd
import psycopg2
from statsmodels.tsa.statespace.mlemodel import MLEModel
from matplotlib import pyplot as plt

#host = os.getenv("PG_HOST")
#database = os.getenv("PG_DATABASE")
#user = os.getenv("PG_USER")
#password = os.getenv("PG_PASSWORD")

# NOTE: hacking it in ipython for now
%run secrets.py

conn = psycopg2.connect(
    host=host, database=database, user=user, password=password, port=25060
)
curs = conn.cursor()

curs.execute(
    """
    select product_id
           , ts
           , close
    from dm.candles
    """
)

df = pd.DataFrame(curs.fetchall())
df.columns = ["product_id", "ts", "close"]
df["close"] = df["close"].astype(float) 

df_wide = df.pivot(index='ts', columns='product_id')

some_complete = df_wide.apply(lambda row: np.mean(~row.isna()) > .25, axis=1)
first_obs = np.where(some_complete)[0][0]

df_wide = df_wide.iloc[first_obs:, ]
df_wide.columns = [c[1].replace('-', '_').replace('1', 'ONE')
                   for c in df_wide.columns]

out_df = df_wide.copy().reset_index()
out_df.to_csv("prices.csv", index=False)

# Start the modeling

hourly = df_wide.loc[df_wide.index.minute == 0][['BTC_USD', 'BCH_USD']]
hourly = np.log(hourly)


class BivariateVECM(MLEModel):
    """ Bivariate Error correction model with 2 lags
        
        Based on the "transitory" specification described on page 11 of
        https://cran.r-project.org/web/packages/urca/urca.pdf

        Using alpha_vec * beta^T as Pi, the error correction matrix

    """
    start_params = [-.05, .012, -.06, -.02,
                    .002, .003, -1.8,
                    .5, .5]
    param_names = ['gamma_11', 'gamma_12', 'gamma_21', 'gamma_22',
                   'alpha_1', 'alpha_2', 'beta',
                   'sigma_y', 'sigma_z']
    state_names = ['a', 'b', 'c', 'd', 'e', 'f']

    def __init__(self, df):
        super().__init__(df.to_numpy()[2:, :], k_states=6)

        if len(df.columns) > 2:
            raise ValueError("Only 2-var VECM is implemented currently")

        # Set up initial state
        self.initialize_known(
            np.array([df.iloc[0, 0], df.iloc[0, 1],
                      df.iloc[1, 0], df.iloc[1, 1],
                      .001, .001]),
            np.diag(np.repeat(10, 6))
        )

    def update(self, params, **kwargs):
        params = super().update(params, **kwargs)

        GAMMA = np.matrix([[params[0], params[1]], [params[2], params[3]]])
        alpha = np.array([[params[4]], [params[5]]])
        betaT = np.array([[1, params[6]]])
        
        PI1 = np.diag([1, 1]) + GAMMA + np.matmul(alpha, betaT)
        PI2 = -GAMMA

        self['transition', 0:2, 0:2] = PI1
        self['transition', 0:2, 2:4] = PI2
        self['transition', 2, 0] = 1
        self['transition', 3, 1] = 1

        self['transition', 4, 4] = 1
        self['transition', 5, 5] = 1

        # state model ------
        self['selection', 0, 0] = 1
        self['selection', 1, 1] = 1

        self['state_cov', 0, 0] = params[7] ** 2
        self['state_cov', 1, 1] = params[8] ** 2

        # observation model ------
        self['design', 0, 0] = 1
        self['design', 1, 1] = 1
        self['design', 0, 4] = 1
        self['design', 1, 5] = 1


mod = BivariateVECM(hourly)
mod.update(mod.start_params)

mod['transition']
mod['state_cov']

mod['design']
mod.initialization.stationary_cov

res = mod.fit(maxiter=200000, method='bfgs')
res.summary()

plt.close()

plt.plot(res.filtered_state[4, :])
plt.plot(res.filtered_state[5, :])

# build the new "initial state" for the simulation

new_initial = (
    np.array([
        hourly.iloc[-1, 0], hourly.iloc[-1, 1],
        hourly.iloc[-2, 0], hourly.iloc[-2, 1],
        res.filtered_state[4, -1],
        res.filtered_state[5, -1]]
    )
)

last_ts = hourly.index[-1]
horizon = 1000
sim_index = pd.date_range(start=hourly.index[-1] + timedelta(hours = 1),
                          periods=horizon, freq='H')

sim_df = pd.DataFrame(res.simulate(horizon, initial_state=new_initial),
                      index=sim_index)

sim_df.columns = ['BTC_USD', 'BCH_USD']


hourly['period'] = 'past'
sim_df['period'] = 'future'

both = pd.concat([hourly, sim_df])

plt.close()
plt.plot(both.BTC_USD, label="BTC-USD")
plt.plot(both.BCH_USD, label="BCH-USD")
plt.axvline(x=last_ts)
plt.legend()
plt.title("Past and simulated future (n=1) according to ECM model")




plt.plot(both.BCH_USD, both.BTC_USD, label="past")

plt.plot(both.loc[both.period=='future'].BCH_USD,
         both.loc[both.period=='future'].BTC_USD, label="future")
plt.xlabel("BCH-USD")
plt.ylabel("BTC-USD")
plt.legend()
plt.title("One simulated path back to equilibrium")
