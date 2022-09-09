import multiprocessing
import functools

import sys
import pandas as pd
import numpy as np
from math import log,sqrt,exp
from scipy import stats
import time
from datetime import datetime


# black scholes model, ref: https://en.wikipedia.org/wiki/Black%E2%80%93Scholes_model
# input: s0, strike, time_to_maturity, interest rate, vol, dividend yield, option type
# output: option price on given vol
def bsm_option_value(s0,k,t,r,sigma,q = 0, TYPE="C"):
    d1 = (np.log(s0/k) + (r - q + 0.5*sigma**2)*t)/(sigma*np.sqrt(t))
    d2 = d1 - sigma*sqrt(t)
    if TYPE == "C":
        value = (s0*np.exp(-q*t)* stats.norm.cdf(d1) - k*np.exp(-r * t)* stats.norm.cdf(d2))
    elif TYPE == "P":
        value = k*exp(-r * t)*stats.norm.cdf(-d2) - s0*exp(-q*t)*stats.norm.cdf(-d1)
    return value

# get vega
# input: s0, strike, time_to_maturity, interest rate, vol
# output: vega
def bsm_vega(s0, k, t,r,sigma):
    d1 = (np.log(s0/k) + (r + 0.5*sigma**2)*t)/(sigma*np.sqrt(t))
    vega = s0*stats.norm.cdf(d1,0.,1.)*np.sqrt(t)
    return vega

# newton raphson method, get vol
# input: s0, strike, time_to_maturity, interest rate, option price, initialized vol, dividend yield, max iter, stop threshold, option type
# output: an implied vol
def bsm_imp_vol_newton(s0, k, t, r, c0, sigma_est=1,q=0, it=100, threshold=0.001, TYPE="C"):
    sigma_est_pre=0
    for i in range(it):
        sigma_est -= (bsm_option_value(s0, k,t,r,round(sigma_est,2),q=q, TYPE=TYPE) - c0) / bsm_vega(s0, k, t, r, round(sigma_est,2))
        if abs(sigma_est-sigma_est_pre) < threshold:
            break
        sigma_est_pre=sigma_est
    return sigma_est

# get time to maturity with a given day or today
# input: option final trading day (expiration day)
# output: remaining days/365
def calculateTime(start,DAY):
    today = datetime.now()
    if start == "today": 
        date1 = today
        date2 = datetime.strptime(DAY, "%m/%d/%Y")
    elif start: 
        date1 = datetime.strptime(start, "%m/%d/%Y")
        date2 = datetime.strptime(DAY, "%m/%d/%Y")
    else:
        print("please set starting day")
    dayNum = (date2 - date1).days
    t = round(dayNum / 365, 3)
    return t

# main function, 
def IV_surface(input_option_file,output_file,s0,interestRate,windowRatio,dividendYield,mode,start,fillna):
    print(start)
    start_strike_price = (1-windowRatio) * s0
    end_strike_price = (1+windowRatio) * s0

    # read option price file
    SPX_data = pd.read_csv(input_option_file)
    SPX_data = SPX_data[(SPX_data["Strike"] > start_strike_price) & (SPX_data["Strike"] < end_strike_price)]

    ExDate = SPX_data["Expiration Date"]

    # option price
    SPX_data["call_bid_ask_mean"] = (SPX_data["Bid_call"] + SPX_data["Ask_call"])/2
    SPX_data["put_bid_ask_mean"] = (SPX_data["Bid_put"] + SPX_data["Ask_put"])/2

    # all unique days, an axis of iv surface
    dateList = np.array(np.unique(ExDate))
    time_new = np.array([calculateTime(start,day) for day in dateList])

    # all strikes, another axis of iv surface
    strike_price_list = np.unique(SPX_data["Strike"].values)

    # container of iv surface
    vol_surface = pd.DataFrame(index=time_new,columns=strike_price_list)

    for date_count in range(len(dateList)):
        date = dateList[date_count]
        df_option_contract=SPX_data[SPX_data['Expiration Date'] == date] # all contracts that expiration day = date
        list_strike_price = df_option_contract["Strike"].values # all strikes
        
        for strike_count in range(len(list_strike_price)):
            strike = list_strike_price[strike_count]

            if mode == "call_only": 
                price=df_option_contract[df_option_contract["Strike"]==strike]["call_bid_ask_mean"].values[0]
                vol=bsm_imp_vol_newton(s0, strike, time_new[date_count], interestRate, price, q=dividendYield, TYPE = "C")
                vol_surface.loc[time_new[date_count],strike]=vol

            elif mode == "put_only":
                price=df_option_contract[df_option_contract["Strike"]==strike]["put_bid_ask_mean"].values[0]
                vol=bsm_imp_vol_newton(s0, strike, time_new[date_count], interestRate, price, q=dividendYield, TYPE = "P")
                vol_surface.loc[time_new[date_count],strike]=vol

            elif mode == "mixed":
                if strike>s0:
                    price=df_option_contract[df_option_contract["Strike"]==strike]["call_bid_ask_mean"].values[0]
                    vol=bsm_imp_vol_newton(s0, strike, time_new[date_count], interestRate, price, q=dividendYield, TYPE = "C")
                    vol_surface.loc[time_new[date_count],strike]=vol
                else:
                    price=df_option_contract[df_option_contract["Strike"]==strike]["put_bid_ask_mean"].values[0]
                    vol=bsm_imp_vol_newton(s0, strike, time_new[date_count], interestRate, price, q=dividendYield, TYPE = "P")
                    vol_surface.loc[time_new[date_count],strike]=vol

    if fillna:
        ix=len(vol_surface.index)
        cs=len(vol_surface.columns)
        for i in range(ix):
            for j in range(cs):
                if (vol_surface.iloc[i,j] is np.nan) and i*j>=1 and i<ix-1 and j<cs-1:
                    vol_surface.iloc[i,j]=(vol_surface.iloc[i+1,j+1]+vol_surface.iloc[i+1,j-1]+vol_surface.iloc[i-1,j+1]+vol_surface.iloc[i-1,j-1])/4

    vol_surface.to_csv(output_file)


if __name__ == "__main__":
    raw_df_50etf_option=pd.read_excel("50ETF_OPTION_CLEAN.xlsx")
    raw_df_50etf = pd.read_excel("50ETF_RAW.xlsx")
    all_date=raw_df_50etf_option["date"].unique()

    pool = multiprocessing.Pool(7)
    manager = multiprocessing.Manager()

    for day in all_date:
        input_option_file = "./slices/50etf_option_"+pd.to_datetime(str(day)).strftime("%Y%m%d%H%M%S")+".csv"
        output_file = "./iv_surface_TimeToMaturityAxis/put_"+pd.to_datetime(str(day)).strftime("%Y%m%d")+".csv"
        s0 = raw_df_50etf[raw_df_50etf["DateTime"]==day]["VWAP"].values[0]
        interestRate=0.03
        windowRatio=0.5
        dividendYield=0
        mode = "put_only" # call_only/put_only/mixed
        start=pd.to_datetime(str(day)).strftime("%m/%d/%Y")
        fillna=False
        # IV_surface(input_option_file,output_file,s0,interestRate,windowRatio,dividendYield,mode,start,fillna)
        pool.apply_async(IV_surface, (input_option_file,output_file,s0,interestRate,windowRatio,dividendYield,mode,start,fillna))

    pool.close()
    print("start")
    start_time = time.time()
    pool.join()
    print("done in",time.time()-start_time,"s")
