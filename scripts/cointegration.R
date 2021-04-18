library(urca)
library(vars)
library(tsDyn)
library(dplyr)
library(readr)
library(lubridate)
library(ggplot2)


df <- read.csv("prices.csv")
df$ts <- as_datetime(df$ts)

hourly <- df %>% filter(minute(ts) == 0) #,hour(ts) == 0 for hourly

ggplot(hourly, aes(x=ts, y=ATOM_USD)) +
    geom_point()

hdf <- log(hourly[, c("BTC_USD", "BCH_USD")])

jotest <- ca.jo(hdf,
                type="eigen",
                ecdet="none",
                K=2,
                spec="transitory")

summary(jotest)

jotest@GAMMA
jotest@V[, 1]  # Beta
jotest@W[, 1]  # Alpha

coinlm <- lm(BTC_USD ~ BCH_USD, data = hdf)
plot(resid(coinlm))

# This does not handle NAs
vecm <- tsDyn::VECM(hdf, lag=2, r=1, estim="ML")


# Quick check: two independent random walks, and two stationary series
N <- 1000
iid_df <- data.frame(x = rnorm(1000), y = rnorm(1000))
irws_df <- data.frame(x = arima.sim(list(order=c(0, 1, 0)), n = 1000),
                      y = arima.sim(list(order=c(0, 1, 0)), n = 1000))
drift <- 0
drifting_rw <- cumsum(rep(drift, N) + rnorm(N))
coint_df <- data.frame(
  x = .5 * drifting_rw + .4 * rnorm(N),
  y = .75 * drifting_rw + .3 * rnorm(N)
)
plot(coint_df$y, type="l")
lines(coint_df$x, col='blue')

# Reject tests that rank is 0, le 1, so it must be 2
jotest <- ca.jo(iid_df,
                type="eigen",
                ecdet="none",
                K=2,
                spec="transitory")

summary(jotest)

# Can't reject test that r = 0, left with two unrelated random walks
jotest <- ca.jo(irws_df,
                type="eigen",
                ecdet="none",
                K=2,
                spec="transitory")

summary(jotest)

# Clear rejection of r = 0, but can't reject r <= 1, so conclude r = 1
jotest <- ca.jo(coint_df,
                type="eigen",
                ecdet="none",
                K=2,
                spec="transitory")

summary(jotest)
jotest@GAMMA[, 2:3]


# TODO: now that you're in a real ECM, try to recover the drifting_rw as well as the loadings on drifting
# http://lenkiefer.com/2019/10/24/forecasts-from-a-bivariate-vecm-conditional-on-one-of-the-variables/
yx1 <- coint_df
names(yx1) <- c("y", "x")
vecm1 <- VECM(yx1,lag=1)

vecm1 %>% summary()

intercept <- VARrep(vecm1)[,1]
Ft <- cbind(diag(1,2),diag(0,2))
Tt <-
    matrix(c(VARrep(vecm1)[1,-1],
                        VARrep(vecm1)[2,-1],
                                   c(1,0,0,0),
                                   c(0,1,0,0)),
                    nrow=4,byrow=TRUE)
Qt <- cov(residuals(vecm1))intercept <- VARrep(vecm1)[,1]
Ft <- cbind(diag(1,2),diag(0,2))
Tt <-
    matrix(c(VARrep(vecm1)[1,-1],
                        VARrep(vecm1)[2,-1],
                                   c(1,0,0,0),
                                   c(0,1,0,0)),
                    nrow=4,byrow=TRUE)
Qt <- cov(residuals(vecm1))


K2 <- SSModel( as.matrix(yx1) ~ -1 +
    # add the intercept 
    SSMcustom(Z = diag(0,2), T = diag(2), Q = diag(0,2), 
              a1 = matrix(intercept,2,1), P1inf = diag(0,2),index=c(1,2),
              state_names=c("mu_y","mu_x"))+
     
     # add the VAR part (excluding intercept)
     
     SSMcustom(Z=Ft,  # observations
               T=Tt, # state transtion (from VECM)
               Q=Qt,  # state innovation variance (from VECM)
               index=c(1,2), # we observe variables 1 & 2 from statespace (without noise)
               state_names=c("y","x","ylag","xlag"),  # name variables
               a1=c(yx1$y[2],yx1$x[2],yx1$y[1], yx1$x[1]),   #initialize values
               P1=1e7*diag(1,4)),  # make initial variance very large

     H=diag(0,2) # no noise in observation equation
)

k
