import time,datetime
import numpy as np
import pandas as pd
import json
import logging
import websocket
from sortedcontainers import SortedDict
from threading import Lock
from sqlalchemy import create_engine
from copy import deepcopy
from ..conf import mysqlconf

LEVEL = 15
HOUR9 = datetime.timedelta(hours=9)
ENDPOINT = "wss://www.bitmex.com/realtime"
BIDCOL = [f"bidPrice{i}" for i in range(1,LEVEL+1)] + [f"bidVol{i}" for i in range(1,LEVEL+1)]
ASKCOL = [f"askPrice{i}" for i in range(1,LEVEL+1)] + [f"askVol{i}" for i in range(1,LEVEL+1)]
BOOKCOL = BIDCOL + ASKCOL
MYSQLURL = mysqlconf.URL + "BitMEX"


class MexTicker:

    def __init__(self, symbol="XBTUSD", save=False):
        self.url = ENDPOINT
        self.symbol = symbol.replace("/","").replace("_","").upper()
        self.logger = logging.getLogger(f"{__file__}_{self.symbol}")
        self.engine = create_engine(MYSQLURL, echo=False)
        self._lock = Lock()
        # flag
        self.save = save
        self._stopFlag = False
        self._book_partialed = False
        
        # MarketBook
        self.time_book = np.array([])
        self.book_bidnow = {} # {id : [price, size]}
        self.book_asknow = {}
        self.book_bid = np.array([]) # snapshot
        self.book_ask = np.array([])
        # MarketActivity
        self.act = {
            "id": np.array([]),   "rcvTime" : np.array([]), "mktTime" : np.array([]), "direction": np.array([]),
            "side": np.array([]), "price"   : np.array([]), "amount"  : np.array([]), "symbol": np.array([])
        }
        # lv1
        self._bestBid = None
        self._bestAsk = None
        self.lv1 = {
            "rcvTime": np.array([]), "midPrice": np.array([]), "symbol": np.array([]),
            "bestBid": np.array([]), "bestAsk": np.array([]), 
        }
        # latency
        self.latency = 0

        # flag_time
        self.time_cut = time.time()
        self.time_push = datetime.datetime.now()
        self.laststack_book = time.time()
        self._hold_time = 30

        self.set_websocket()
    

    def set_websocket(self):
        def on_message(ws, message):
            message = json.loads(message)
            if not "table" in message.keys(): return None
            elif message["table"] == "orderBookL2_25": self.on_message_book25(message)
            elif message["table"] == "trade": self.on_message_trade(message)
        def on_open(ws):
            ws.send(json.dumps({
                "op": "subscribe", 
                "args": [f"{ch}:{self.symbol}" for ch in ["trade", "orderBookL2_25"]] }))
            self.logger.info("BITMEX Websocket connected.")
        def on_error(ws, error):
            self.logger.info("BITMEX Websocket error.")
            self.logger.error(error)
        def on_close(ws):
            self.logger.info("BITMEX Websocket closed.")

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
        self._stopFlag = True
        self.engine.dispose()
        self.ws.close()
        self.logger.info("called close().")


    ### on_message
    def on_message_trade(self, message):
        """ trade messageのfeedhandler
        Args:
            message (dict): {data: [{"side":, "price":, ...}, {}, {}]}
        """
        message = message["data"]
        # 必要な情報をndarrayで格納 (*板寄せ時はside情報がBlankになる)
        ids = np.array([self.getId() for m in message])
        side  = np.array([m["side"].lower() for m in message])
        price = np.array([m["price"] for m in message])
        amount  = np.array([m["size"] for m in message])
        symbol = np.array([m["symbol"] for m in message])
        direction = np.array([m["tickDirection"] for m in message])
        mktTime = np.array([datetime.datetime.strptime(m["timestamp"][:26].replace("Z","").replace("T"," "), '%Y-%m-%d %H:%M:%S.%f') + HOUR9 for m in message])
        rcvTime = mktTime.copy(); rcvTime[:] = datetime.datetime.now()
        with self._lock:
            # append
            self.act["id"] = np.append(self.act["id"], ids)
            self.act["rcvTime"] = np.append(self.act["rcvTime"], rcvTime)
            self.act["mktTime"] = np.append(self.act["mktTime"], mktTime)
            self.act["side"] = np.append(self.act["side"], side)
            self.act["price"] = np.append(self.act["price"], price)
            self.act["amount"] = np.append(self.act["amount"], amount)
            self.act["symbol"] = np.append(self.act["symbol"], symbol)
            self.act["direction"] = np.append(self.act["direction"], direction)
            self.latency = (rcvTime[-1] - mktTime[-1]).total_seconds()


    def on_message_book25(self, message):
        """ l2_25 messageのfeedhandler
        """
        if message["action"] == "partial": self.partial_board(message)
        elif message["action"] == "update": self.update_board(message)
        elif message["action"] == "delete": self.delete_board(message)
        elif message["action"] == "insert": self.insert_board(message)
        # snapshotの更新
        if time.time()-self.laststack_book>0.1 and len(self.book_asknow)>LEVEL and len(self.book_bidnow)>LEVEL:
            with self._lock:
                self.book_bid  = np.append(self.book_bid, self.get_board("bid",LEVEL).reshape(1,-1), axis=0) if self.book_bid.shape[0]>0 else self.get_board("bid",LEVEL).reshape(1,-1)
                self.book_ask  = np.append(self.book_ask, self.get_board("ask",LEVEL).reshape(1,-1), axis=0) if self.book_ask.shape[0]>0 else self.get_board("ask",LEVEL).reshape(1,-1)
                self.time_book = np.append(self.time_book, datetime.datetime.now())
                self.laststack_book = time.time()
        # lv1の更新
        if self._bestBid != self.bestBid or self._bestAsk != self.bestAsk:
            with self._lock:
                self._bestBid = self.bestBid
                self._bestAsk = self.bestAsk
                self.lv1["rcvTime"] = np.append(self.lv1["rcvTime"], datetime.datetime.now())
                self.lv1["bestBid"] = np.append(self.lv1["bestBid"], self._bestBid)
                self.lv1["bestAsk"] = np.append(self.lv1["bestAsk"], self._bestAsk)
                self.lv1["midPrice"] = np.append(self.lv1["midPrice"], (self._bestBid + self._bestAsk)/2)
                self.lv1["symbol"] = np.append(self.lv1["symbol"], self.symbol)


    def manage_data(self):
        """ DBへのpushおよび過去Dataのdrop
        """
        while not self._stopFlag:
            now = datetime.datetime.now()
            try:
                # drop(hold_time secに1回)
                if time.time() - self.time_cut > self._hold_time:
                    self.logger.debug("try drop")
                    with self._lock:
                        kiritoriTime = datetime.datetime.now() - datetime.timedelta(seconds=self._hold_time)
                        self.act = self.extract_from_dict(deepcopy(self.act), "rcvTime", kiritoriTime)
                        self.lv1 = self.extract_from_dict(deepcopy(self.lv1), "rcvTime", kiritoriTime)
                        self.time_book, self.book_bid, self.book_ask = \
                            self.extract_from_list([self.time_book, self.book_bid, self.book_ask], kiritoriTime)
                        self.time_cut = time.time()

                # 書き込み
                if self.save and now - self.time_push > datetime.timedelta(seconds=10):
                    self.logger.debug("try push")
                    with self._lock:
                        act = self.extract_from_dict(deepcopy(self.act), "rcvTime", self.time_push)
                        lv1 = self.extract_from_dict(deepcopy(self.lv1), "rcvTime", self.time_push)
                        book  = self.extract_from_list(deepcopy([self.time_book, self.book_bid, self.book_ask]), self.time_push)
                    # 直近の記録時間を更新する.
                    self.to_MySQL(act, book, lv1)
                    self.time_push = now
            except Exception as e:
                self.logger.info(e, exc_info=True)
            time.sleep(1.0)


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
                    np.concatenate((book[0].reshape(-1,1), book[1], book[2]), axis=1), 
                    columns=["rcvTime"]+BOOKCOL)
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
            self.logger.error(f"{e}")
            con.close()
            self.engine.dispose()


    ### OrderBook Methods
    def partial_board(self, message):
        # 板をすげ替える
        self.logger.debug("partial")
        book_asknow = SortedDict()
        book_bidnow = SortedDict()
        for m in message["data"]:
            if m["side"] == "Sell":
                book_asknow[m["id"]] = [m["price"], m["size"]]
            elif m["side"] == "Buy":
                book_bidnow[m["id"]] = [m["price"], m["size"]]
        self.book_asknow = book_asknow
        self.book_bidnow = book_bidnow
        self._book_partialed = True

    def update_board(self, message):
        # Volumeのみ更新
        if _book_partialed:
            for m in message["data"]:
                if m["side"] == "Sell":
                    self.book_asknow[m["id"]][1] = m["size"]
                elif m["side"] == "Buy":
                    self.book_bidnow[m["id"]][1] = m["size"]

    def delete_board(self, message):
        # 消す
        if self._book_partialed:
            for m in message["data"]:
                if m["side"] == "Sell":
                    try: self.book_asknow.pop(m["id"])
                    except: pass
                elif m["side"] == "Buy":
                    try: self.book_bidnow.pop(m["id"])
                    except: pass

    def insert_board(self, message):
        # 挿入
        if self._book_partialed:
            for m in message["data"]:
                if m["side"] == "Sell":
                    self.book_asknow[m["id"]] = [m["price"], m["size"]]
                elif m["side"] == "Buy":
                    self.book_bidnow[m["id"]] = [m["price"], m["size"]]


    def get_board(self, side="bid", level=10):
        # 近傍10levelを [price,...,size,,...] で返す.
        if side == "bid":
            res = np.array(self.book_bidnow.values())
            res = res[np.argsort(res[:,0])][::-1][:level].flatten("F")
        if side == "ask":
            res = np.array(self.book_asknow.values())
            res = res[np.argsort(res[:,0])][::+1][:level].flatten("F")
        return res


    @property
    def bestBid(self):
        # return self.book_bidnow.peekitem(-1)[0]
        return np.array(self.book_bidnow.values())[:, 0].max() if len(self.book_bidnow) > 0 else None

    @property
    def bestAsk(self):
        # return self.book_asknow.peekitem(0)[0]
        return np.array(self.book_asknow.values())[:, 0].min() if len(self.book_asknow) > 0 else None

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
        return str(time.time_ns())