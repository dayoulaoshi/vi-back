import tornado.web

import numpy as np

import sys
import pandas as pd
import datetime
import time
import random



class BaseHandler(tornado.web.RequestHandler):
    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header("Content-Type", "application/json; charset=UTF-8")
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Allow-Headers',
                        'authorization, Authorization, Content-Type, Access-Control-Allow-Origin, Access-Control-Allow-Headers, X-Requested-By, Access-Control-Allow-Methods')



class dev_ivsurface(BaseHandler):
    def options(self):
        self.finish()

    def get(self):
        start=time.time()
        raw_df_50etf = pd.read_excel("FileVaults/50ETF_RAW.xlsx", engine='openpyxl')
        all_date = raw_df_50etf["DateTime"].unique()
        day = random.choice(all_date)
        target = "FileVaults/iv_surface_TimeToMaturityAxis/put_" + pd.to_datetime(str(day)).strftime("%Y%m%d") + ".csv"
        df_raw_target = pd.read_csv(target)
        df_raw_target.index=df_raw_target.iloc[:,0]
        return_res = []
        return_index = []
        for index in df_raw_target.index:
            for col in df_raw_target.columns[1:]:
                if pd.notna(df_raw_target.loc[index,col]) and 0<df_raw_target.loc[index,col]:
                    return_res.append([float(index),float(col),float(df_raw_target.loc[index,col])])
                return_index.append([float(index),float(col)])
                # print(float(index),float(col))
        print("return in ",time.time()-start,"s")
        self.finish({"code": 0, "msg": "get ok","total": len(return_res), "data":{"index": return_index,"array": return_res}})


class dev_ivsmile(BaseHandler):
    def options(self):
        self.finish()

    def get(self):
        array = np.random.rand(100).tolist()
        index = [i for i in range(100)]
        self.finish({"code": 0, "msg": "get ok","total":len(array), "data":{"index":index,"array":array}})


class dev_ivtermstructure(BaseHandler):
    def options(self):
        self.finish()

    def get(self):
        array = np.random.rand(100).tolist()
        index = [i for i in range(100)]
        self.finish({"code": 0, "msg": "get ok","total":len(array), "data":{"index":index,"array":array}})


class dev_50etf(BaseHandler):
    def options(self):
        self.finish()

    def get(self):
        raw_df_50etf = pd.read_excel("FileVaults/50ETF_RAW.xlsx", engine='openpyxl')
        all_date = raw_df_50etf["DateTime"].unique()
        self.finish({"code": 0, "msg": "get ok"})


class dev_ss300(BaseHandler):
    def options(self):
        self.finish()

    def iter_points(self, tar_df):
        tar_df.index = tar_df["Unnamed: 0"]
        temp = []
        for exp in tar_df.index:
            for strike in tar_df.columns[1:]:
                temp.append([exp,strike,float(tar_df.loc[exp,strike]) if tar_df.loc[exp,strike]<1 else 1])
        return temp

    def get(self):
        count = eval(self.get_argument("count","0"))

        raw_df_ss300 = pd.read_excel("FileVaults/ss300.xlsx", engine='openpyxl')
        raw_df_meta = pd.read_excel("FileVaults/metadata.xlsx", engine='openpyxl')
        this_row_ss300 = raw_df_ss300.iloc[count].to_dict()
        this_row_ss300["DateTime"] = pd.to_datetime(this_row_ss300["DateTime"]).strftime("%Y/%m/%d %H:%M:%S")

        this_row_meta = raw_df_meta.iloc[count].to_dict()
        this_row_meta["DateTime"] = pd.to_datetime(this_row_meta["DateTime"]).strftime("%Y/%m/%d %H:%M:%S")
        call_filename = this_row_meta["call"].replace("./", "FileVaults/")
        put_filename = this_row_meta["put"].replace("./", "FileVaults/")
        gap_filename = this_row_meta["gap"].replace("./", "FileVaults/")

        call_points_list = self.iter_points(pd.read_excel(call_filename, engine='openpyxl'))
        put_points_list = self.iter_points(pd.read_excel(put_filename, engine='openpyxl'))
        gap_points_list = self.iter_points(pd.read_excel(gap_filename, engine='openpyxl'))
        self.finish({"code": 0, "msg": "get ok","count":count,"data":{"ss300":this_row_ss300,"call_ivs":call_points_list,"put_ivs":put_points_list,"gap_ivs":gap_points_list}})




### SPY ###

df_meta_raw = pd.read_pickle("FileVaults/DemoData/meta_20210429.pkl")

class dev_SPY(BaseHandler):
    def options(self):
        self.finish()

    def iter_points(self, tar_df, now):
        temp = []
        for exp in tar_df.index:
            maturity = (datetime.datetime.strptime(str(exp),"%Y%m%d")+datetime.timedelta(hours=16,minutes=30)-now)/datetime.timedelta(days=365)
            for strike in tar_df.columns:
                temp.append([maturity,eval(strike),float(tar_df.loc[exp,strike])])
        return temp

    def get(self):
        count = eval(self.get_argument("count","0"))

        this_row= df_meta_raw.iloc[count]
        this_row_datetime = this_row["DATETIME"]

        this_row_spy=dict()

        # this_row_spy["DateTime"] = this_row_datetime.strftime("%Y/%m/%d %H:%M:%S")
        this_row_spy["DateTime"] = count
        this_row_spy["open"] = this_row["UNDERLYING"]
        this_row_spy["high"] = this_row["UNDERLYING"]
        this_row_spy["low"] = this_row["UNDERLYING"]
        this_row_spy["close"] = this_row["UNDERLYING"]


        call_filename = "FileVaults"+this_row["CALL"]
        put_filename = "FileVaults"+this_row["PUT"]
        gap_filename = "FileVaults"+this_row["GAP"]

        call_points_list = self.iter_points(pd.read_pickle(call_filename),this_row_datetime)
        put_points_list = self.iter_points(pd.read_pickle(put_filename),this_row_datetime)
        gap_points_list = self.iter_points(pd.read_pickle(gap_filename),this_row_datetime)

        self.finish({"code": 0, "msg": "get ok","count":count,"data":{"ss300":this_row_spy,"call_ivs":call_points_list,"put_ivs":put_points_list,"gap_ivs":gap_points_list}})

class dev_full_SPY(BaseHandler):
    def options(self):
        self.finish()

    def iter_raw_points(self, tar_df):
        temp = []
        for i_exp in range(len(tar_df.index)):
            for i_strike in range(len(tar_df.columns[1:])):
                temp.append([i_exp,i_strike,float(tar_df.iloc[i_exp,i_strike])])
        return temp, tar_df.index.values.tolist(), tar_df.columns.values.tolist(),

    def iter_points(self, tar_df, now):
        temp = []
        for exp in tar_df.index:
            maturity = (datetime.datetime.strptime(str(exp),"%Y%m%d")+datetime.timedelta(hours=16,minutes=30)-now)/datetime.timedelta(days=365)
            for strike in tar_df.columns:
                temp.append([maturity,eval(strike),float(tar_df.loc[exp,strike])])
        return temp

    def get(self):
        print(df_meta_raw)
        count = eval(self.get_argument("count","0"))

        this_row= df_meta_raw.iloc[count]
        this_row_datetime = this_row["DATETIME"]

        this_row_spy=dict()

        this_row_spy["DateTime"] = this_row_datetime.strftime("%Y/%m/%d %H:%M:%S")
        this_row_spy["open"] = this_row["UNDERLYING"]
        this_row_spy["high"] = this_row["UNDERLYING"]
        this_row_spy["low"] = this_row["UNDERLYING"]
        this_row_spy["close"] = this_row["UNDERLYING"]


        call_filename = "FileVaults"+this_row["CALL"]
        put_filename = "FileVaults"+this_row["PUT"]
        gap_filename = "FileVaults"+this_row["GAP"]

        call_points_list = self.iter_points(pd.read_pickle(call_filename),this_row_datetime)
        put_points_list = self.iter_points(pd.read_pickle(put_filename),this_row_datetime)
        gap_points_list = self.iter_points(pd.read_pickle(gap_filename),this_row_datetime)

        hm_data, hm_x, hm_y = self.iter_raw_points(pd.read_pickle(call_filename))

        self.finish({
            "code": 0,
            "msg": "get ok",
            "count":count,
            "data":{
                "ss300":this_row_spy,
                "call_ivs":call_points_list,
                "put_ivs":put_points_list,
                "gap_ivs":gap_points_list,
                },
            "heatmapC":{
                "x":hm_x,
                "y":hm_y,
                "data":hm_data,
            },
        })

class testapi(tornado.web.RequestHandler):
    def get(self):
        self.render('../static/index.html')



class ivsurface(tornado.web.RequestHandler):

    def get(self):
        array = np.random.rand(100,3)
        # print(array)
        index=[array[:,0].tolist(), array[:,1].tolist()]
        self.finish({"code": 0, "msg": "get ok","total":len(array), "data":{"index":index,"array":array.tolist()}})


class ivsmile(tornado.web.RequestHandler):

    def get(self):
        array = np.random.rand(100).tolist()
        index = [i for i in range(100)]
        self.finish({"code": 0, "msg": "get ok","total":len(array), "data":{"index":index,"array":array}})


class ivtermstructure(tornado.web.RequestHandler):

    def get(self):
        array = np.random.rand(100).tolist()
        index = [i for i in range(100)]
        self.finish({"code": 0, "msg": "get ok","total":len(array), "data":{"index":index,"array":array}})
