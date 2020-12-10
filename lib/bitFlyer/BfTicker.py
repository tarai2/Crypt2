import time,datetime
import numpy as np
import pandas as pd
import json
import logging
import itertools
import websocket
from sortedcontainers import SortedDict
from threading import Lock
from sqlalchemy import create_engine
from copy import deepcopy
from ..conf import mysqlconf
from .DataSchema import LEVEL, DBNAME

websocket.setdefaulttimeout(10)
HOUR9 = datetime.timedelta(hours=9)
ENDPOINT = "wss://ws.lightstream.bitflyer.com/json-rpc"
BIDCOL = [f"bidPrice{i}" for i in range(1,LEVEL+1)] + [f"bidVol{i}" for i in range(1,LEVEL+1)]
ASKCOL = [f"askPrice{i}" for i in range(1,LEVEL+1)] + [f"askVol{i}" for i in range(1,LEVEL+1)]
BOOKCOL = BIDCOL + ASKCOL
MYSQLURL = mysqlconf.URL + DBNAME



class BfTicker:
    def __init__(self, symbol="XBTUSD", save=False, hold_time=30):
        self.symbol = symbol
        self.logger = logging.getLogger(f"Bfsocket.{self.symbol}")
        self.engine = create_engine(MYSQLURL, echo=False)
        self._lock = Lock()
        self._hold_time = hold_time
        
        # flag
        self.save = save
        self._book_partialed = False
        
        # MarketBook
        self.book_bidnow = SortedDict() # {id : [price, size]}
        self.book_asknow = SortedDict()
        self.book = {
            "id": np.array([self.getId()]),
            "rcvTime": np.array([datetime.datetime.now()]),
            "bid": np.ones([1, LEVEL*2])*np.nan,
            "ask": np.ones([1, LEVEL*2])*np.nan
        }
        # MarketActivity
        self.act = {
            "id": np.array([]), "symbol": np.array([]),
            "rcvTime": np.array([]), "mktTime": np.array([]),
            "side"   : np.array([]), "price"  : np.array([]), "amount": np.array([]),
            "buy_order_id": np.array([]), "sell_order_id": np.array([])
        }
        # lv1
        self._bestBid = None
        self._bestAsk = None
        self._mid_last = 0
        self.lv1 = {
            "id": np.array([]), "symbol": np.array([]),
            "rcvTime": np.array([]), "midPrice": np.array([]),
            "bestBid": np.array([]), "bestAsk": np.array([])
        }
        # latency
        self.latency = 0

        # flag_time
        self.time_cut = time.time()
        self.time_push = datetime.datetime.now()
        self._time_update_snapshot = time.time()


    def close(self):
        self.engine.dispose()
        self.logger.info("called close().")


    ### on_message
    def on_message_act(self, message):
        self.logger.debug("exec message")
        t = datetime.datetime.now()
        side_arr = np.array([deal["side"].lower() for deal in message])
        price_arr = np.array([deal["price"] for deal in message])
        amount_arr = np.array([deal["size"] for deal in message])
        mktTime_arr = np.array([self.date_fmt(deal["exec_date"]) for deal in message])
        rcvTime_arr = np.array([t for deal in message])
        with self._lock:
            self.act["id"] = np.append(self.act["id"], np.array([self.getId() for i in range(len(message))]))
            self.act["symbol"] = np.append(self.act["symbol"], [self.symbol for i in amount_arr])
            self.act["side"] = np.append(self.act["side"], side_arr)
            self.act["price"] = np.append(self.act["price"], price_arr)
            self.act["amount"] = np.append(self.act["amount"], amount_arr)
            self.act["mktTime"] = np.append(self.act["mktTime"], mktTime_arr)
            self.act["rcvTime"] = np.append(self.act["rcvTime"], rcvTime_arr)
            self.act["buy_order_id"] = np.append(self.act["buy_order_id"], np.array([deal["buy_child_order_acceptance_id"] for deal in message]))
            self.act["sell_order_id"] = np.append(self.act["sell_order_id"], np.array([deal["sell_child_order_acceptance_id"] for deal in message]))
            self.latency = (t - self.act["mktTime"][-1]).total_seconds()
            self.logger.debug(self.latency)


    def on_message_partial(self, message):
        t = datetime.datetime.now()
        # 辞書更新
        self.merge(self.book_bidnow, message["bids"])
        self.merge(self.book_bidnow, message["asks"])
        # snapshot更新
        bids = self.get_board("bid", LEVEL)
        asks = self.get_board("ask", LEVEL)
        with self._lock:
            self.book["id"] = np.append(self.book["id"], self.getId())
            self.book["rcvTime"] = np.append(self.book["rcvTime"], t)
            self.book["bid"] = np.append(self.book["bid"], bids.reshape(1,-1), axis=0)
            self.book["ask"] = np.append(self.book["ask"], asks.reshape(1,-1), axis=0)
            self.logger.debug("patial message processed")


    def on_message_diff(self, message):
        self.logger.debug("diff message")
        t = datetime.datetime.now()
        with self._lock:
            # 辞書更新
            self.merge(self.book_bidnow, message["bids"])
            self.merge(self.book_asknow, message["asks"])
            # LV1保存
            if message["mid_price"]!=self._mid_last:
                self.lv1["id"] = np.append(self.lv1["id"], self.getId())
                self.lv1["rcvTime"] = np.append(self.lv1["rcvTime"], t)
                self.lv1["bestBid"] = np.append(self.lv1["bestBid"], self.bestBid)
                self.lv1["bestAsk"] = np.append(self.lv1["bestAsk"], self.bestAsk)
                self.lv1["midPrice"] = np.append(self.lv1["midPrice"], float(message["mid_price"]))
                self.lv1["symbol"] = np.append(self.lv1["symbol"], self.symbol)
                self._mid_last = message["mid_price"]
            self.logger.debug("update lv1")

        # snapshot更新
        if time.time() - self._time_update_snapshot > 0.1:
            with self._lock:
                bids = self.get_board("bid", LEVEL)
                asks = self.get_board("ask", LEVEL)
                self.logger.debug("get board")
                if bids.shape[0]>2*LEVEL-1 and asks.shape[0]>2*LEVEL-1:
                    self.book["id"] = np.append(self.book["id"], self.getId())
                    self.book["rcvTime"] = np.append(self.book["rcvTime"], t)
                    self.book["bid"] = np.append(self.book["bid"], bids.reshape(1,-1), axis=0)
                    self.book["ask"] = np.append(self.book["ask"], asks.reshape(1,-1), axis=0)
                    self._time_update_snapshot = time.time()
                self.logger.debug(self.lv1["midPrice"][-1])
        # strategyの実行
        # if self.on_board_handler is not None:
        #     await self.on_board_handler(message) #message["id","bids","asks"]


    def merge(self, pre, message):
        # 板差分をmerge.
        for diff in message:
            if diff["size"] > 0:
                pre[diff["price"]] = diff["size"]
            else:
                try: pre.pop(diff["price"])
                except: pass


    def manage_data(self):
        """ DBへのpushおよび過去Dataのdrop
        """
        now = datetime.datetime.now()
        try:
            # drop(hold_time secに1回)
            if time.time() - self.time_cut > self._hold_time:
                self.logger.debug("try drop")
                with self._lock:
                    kiritoriTime = datetime.datetime.now() - datetime.timedelta(seconds=self._hold_time)
                    self.act = self.extract_from_dict(deepcopy(self.act), "rcvTime", kiritoriTime)
                    self.lv1 = self.extract_from_dict(deepcopy(self.lv1), "rcvTime", kiritoriTime)
                    self.book["rcvTime"], self.book["id"], self.book["bid"], self.book["ask"] = \
                        self.extract_from_list([self.book["rcvTime"], self.book["id"], self.book["bid"], self.book["ask"]], kiritoriTime)
                    self.time_cut = time.time()

            # 書き込み
            if self.save and now - self.time_push > datetime.timedelta(seconds=10):
                self.logger.debug("try push")
                with self._lock:
                    act = self.extract_from_dict(deepcopy(self.act), "rcvTime", self.time_push)
                    lv1 = self.extract_from_dict(deepcopy(self.lv1), "rcvTime", self.time_push)
                    book  = self.extract_from_list(deepcopy([self.book["rcvTime"], self.book["bid"], self.book["ask"], self.book["id"]]), self.time_push)
                # 直近の記録時間を更新する.
                self.to_MySQL(act, book, lv1)
                self.time_push = now
        except Exception as e:
            self.logger.info(e, exc_info=True)
        time.sleep(3.0)


    def to_MySQL(self, act: dict, book: list, lv1: dict):
        """ DBへのPUSH
        Args:
            act (dict): [description]
            book (list): [description]
            lv1 (dict): [description]
        """
        try:
            # DFの作成
            df_t = pd.DataFrame(act)
            df_lv1 = pd.DataFrame(lv1)
            try:
                df_book = pd.DataFrame(
                    np.concatenate((book[0].reshape(-1,1), book[1], book[2], book[3].reshape(-1,1)), axis=1), 
                    columns=["rcvTime"]+BOOKCOL+["id"])
                df_book["symbol"] = self.symbol
            except:
                df_book = pd.DataFrame(columns=BOOKCOL)
            
            # dbにpush
            con = self.engine.connect()
            df_book.to_sql("market_book", con, if_exists='append', index=False)
            df_t.to_sql("market_activity", con, if_exists='append', index=False)
            df_lv1.to_sql("lv1", con, if_exists='append', index=False)
            con.close()
            self.engine.dispose()
            
        except Exception as e:
            self.logger.error(e, exc_info=False)
            con.close()
            self.engine.dispose()


    def get_board(self, side="bid", level=10):
        # 近傍10levelを [price,...,size,,...] で返す.
        if side == "bid":
            res = np.array(self.book_bidnow.items())
            res = res[np.argsort(res[:,0])][::-1][:level].flatten("F")
        if side == "ask":
            res = np.array(self.book_asknow.items())
            res = res[np.argsort(res[:,0])][::+1][:level].flatten("F")
        return res

    @property
    def bestBid(self):
        return self.book_bidnow.peekitem(-1)[0] if len(self.book_bidnow) > 0 else None

    @property
    def bestAsk(self):
        return self.book_asknow.peekitem(0)[0] if len(self.book_asknow) > 0 else None

    @property
    def midPrice(self):
        return (self.bestBid + self.bestAsk) / 2

    @property
    def spread(self):
        return (self.bestAsk - self.bestBid)

    ###
    def extract_from_dict(self, old_dict, target_key, time_from):
        """ ndarrayの辞書からtime_fromより現在に近いデータのみ抽出してdictで返す.
            (10us/call in 6keys 300elems each)
            Args:
                old_dict (dict): nd.arrayの配列.少なくとも1つのdatetime配列を含む.
                target_key (str): e.g. rcvTime.
                time_from (datetime): 切り取る境目の時間.
        """
        new = {}
        idx = old_dict[target_key] > time_from
        for _key in old_dict.keys(): new[_key] = old_dict[_key][idx]
        return new


    def extract_from_list(self, rawList, time_from):
        """ [time, data1, data2, ...]から,timeをidxとしてtime_fromより現在に近いデータのみ取り出しlistで返す. """
        res = [ l[rawList[0] > time_from] for l in rawList ]
        if np.diff(np.array([item.shape[0] for item in res])).all() == 0:
            # 全ての配列長が同じなら切り取った値を返す
            return res
        else:
            # 配列長が異なる場合
            raise IndexError

    @staticmethod
    def getId():
        return str(time.time_ns()) + str(np.random.randint(9))

    @staticmethod
    def date_fmt(strdate):
        strdate = '{:0<23}'.format(strdate.replace("T"," ").replace("Z","")[:23])
        return datetime.datetime.strptime(strdate[:19]+"."+strdate[20:], "%Y-%m-%d %H:%M:%S.%f")+HOUR9


class Bfsocket:
    """ WebsocketAppを立ち上げてmessageを受信. 各symbolのTickerにmessageを流す.
    """
    def __init__(self, symbols=["FX_BTC_JPY"], save=False):
        assert isinstance(symbols, list)
        self.url = ENDPOINT
        self.symbols = symbols
        self.logger = logging.getLogger("Bfsocket")
        self._stopFlag = False

        # sym毎のtickerを設定
        self.tickers = {sym: BfTicker(sym, save) for sym in self.symbols}
        self.set_websocket()


    def set_websocket(self):
        
        # 接続先channelと対応するhandlerの作成
        channels = [
            [f"lightning_executions_{sym}", f"lightning_board_{sym}", f"lightning_board_snapshot_{sym}"] 
            for sym in self.symbols]

        handlers = {}
        for i,ch in enumerate(channels):
            ticker = list(self.tickers.values())[i]
            handlers[ch[0]] = ticker.on_message_act
            handlers[ch[1]] = ticker.on_message_diff
            handlers[ch[2]] = ticker.on_message_partial

        def on_message(ws, message):
            message = json.loads(message)
            handlers[message["params"]["channel"]](message["params"]["message"])

        def on_open(ws):
            _channels = [e for inner_list in channels for e in inner_list]  # raze
            params = [
                {'method': 'subscribe', 'params': {'channel': c}} for c in _channels]
            ws.send(json.dumps(params))
            self.logger.info("BitFlyer Websocket connected.")

        def on_error(ws, error):
            self.logger.info("BitFlyer Websocket error.")
            self.logger.error(error, exc_info=True)

        def on_close(ws):
            self.logger.info("BitFlyer Websocket closed.")

        self.ws = websocket.WebSocketApp(self.url,
            on_message=on_message, on_open=on_open, on_error=on_error, on_close=on_close)


    def run(self):
        while not self._stopFlag:
            try:
                self.ws.run_forever()
                time.sleep(1.0)
            except KeyboardInterrupt as e:
                self.close()
                break


    def close(self):
        self.logger.info("called close().")
        self.ws.close()
        self.stopFlag = True
        for sym,tick in self.tickers.items():
            tick.close()


    def manage_data(self):
        while not self._stopFlag:
            for ticker in self.tickers.values():
                ticker.manage_data()
