import time,datetime
import pandas as pd
import numpy as np
import json
import logging
import websocket
from copy import deepcopy


HOUR9 = datetime.timedelta(hours=9)
LEVEL = 3


class MexTicker:

    def __init__(self, symbol="XBTUSD"):
        self.url = "wss://www.bitmex.com/realtime"
        self.symbol = symbol.replace("/","").replace("_","").upper()
        self.logger = logging.getLogger(f"{__file__}_{self.symbol}")
        # flag
        self.save = False
        self._stopFlag = False
        
        # MarketBook
        self.time_book = np.array([])
        self.book_bidnow = {} # {id : [price, size]}
        self.book_asknow = {}
        self.book_bid = np.array([]) # snapshot
        self.book_ask = np.array([])
        # MarketActivity
        self.act = {
            "rcvTime" : np.array([]), "mktTime" : np.array([]),
            "side"    : np.array([]), "price"   : np.array([]), "amount" : np.array([])
        }
        # lv1
        self._bestBid = 0
        self._bestAsk = 0
        self.lv1 = {
            "rcvTime": np.array([]), "midPrice": np.array([]),
            "bestBid": np.array([]), "bestAsk": np.array([]), 
        }
        # latency
        self.latency = 0

        # flag_time
        self.time_cut = time.time()
        self.time_push = datetime.datetime.now()
        self.laststack_book = time.time()

        self.setWebsocket()
    

    def setWebsocket(self):
        def on_message(ws, message):
            message = json.loads(message)
            if not "table" in message.keys():
                return None
            elif message["table"] == "orderBookL2_25": self.on_message_book25(message)
            elif message["table"] == "trade": self.on_message_exec(message)
        def on_open(ws):
            ws.send(json.dumps({"op": "subscribe", "args": ["trade:"+self.symbol]}))
            ws.send(json.dumps({"op": "subscribe", "args": ["orderBookL2_25:"+self.symbol]}))
            if self.logger is not None: self.logger.info("BITMEX Websocket connected.")
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
                break


    ### on_message
    def on_message_exec(self, message):
        # try:
        message = message["data"]
        # 必要な情報をndarrayで格納(板寄せ時はside情報がBlankになる)
        side  = np.array([m["side"] for m in message])
        price = np.array([m["price"] for m in message])
        amount  = np.array([m["size"] for m in message])
        mktTime = np.array([datetime.datetime.strptime(m["timestamp"][:26].replace("Z","").replace("T"," "), '%Y-%m-%d %H:%M:%S.%f') + HOUR9 for m in message])
        rcvTime = mktTime.copy(); rcvTime[:] = datetime.datetime.now()
        # append
        self.act["rcvTime"] = np.append(self.act["rcvTime"], rcvTime)
        self.act["mktTime"] = np.append(self.act["mktTime"], mktTime)
        self.act["side"] = np.append(self.act["side"], side)
        self.act["price"] = np.append(self.act["price"], price)
        self.act["amount"] = np.append(self.act["amount"], amount)
        self.latency = (rcvTime[-1] - mktTime[-1]).total_seconds()
        print("t")


    def on_message_book25(self, message):
        if message["action"]  == "partial": self.partial_board(message)
        elif message["action"] == "update": self.update_board(message)
        elif message["action"] == "delete": self.delete_board(message)
        elif message["action"] == "insert": self.insert_board(message)
        # snapshotの更新
        if time.time()-self.laststack_book>0.1 and len(self.book_asknow)>LEVEL and len(self.book_bidnow)>LEVEL:
            self.book_bid  = np.append(self.book_bid, self.get_board("bid",LEVEL).reshape(1,-1), axis=0) if self.book_bid.shape[0]>0 else self.get_board("bid",LEVEL).reshape(1,-1)
            self.book_ask  = np.append(self.book_ask, self.get_board("ask",LEVEL).reshape(1,-1), axis=0) if self.book_ask.shape[0]>0 else self.get_board("ask",LEVEL).reshape(1,-1)
            self.time_book = np.append(self.time_book, datetime.datetime.now())
            self.laststack_book = time.time()
        # lv1の更新
        if self._bestBid != self.book_bid[-1,0] or self._bestAsk != self.book_ask[-1,0]:
            self.lv1["rcvTime"] = np.append(self.lv1["rcvTime"], datetime.datetime.now())
            self.lv1["bestBid"] = np.append(self.lv1["bestBid"], self.book_bid[-1,0])
            self.lv1["bestAsk"] = np.append(self.lv1["bestAsk"], self.book_ask[-1,0])
            self.lv1["midPrice"] = np.append(self.lv1["midPrice"], self.book_ask[-1,0]/2 + self.book_bid[-1,0]/2)
            self._bestBid = self.lv1["bestBid"][-1]
            self._bestAsk = self.lv1["bestAsk"][-1]


    # DBへのpush
    def manage_data(self):
        while not self._stopFlag:
            now = datetime.datetime.now()

            # drop(10secに1回)
            if time.time() - self.time_cut > 10:
                kiritoriTime = datetime.datetime.now() - datetime.timedelta(seconds=10)
                act = self.extract_from_dict(deepcopy(self.act), "rcvTime", kiritoriTime)
                self.time_book, self.book_bid, self.book_ask = self.extract_from_list([self.time_book, self.book_bid, self.book_ask], kiritoriTime)
                self.time_cut = time.time()

            # 書き込み(30秒に1回)
            if now - self.time_push > datetime.timedelta(seconds=30) and self.save:
                act = self.extract_from_dict(deepcopy(self.act), self.time_push)
                book  = self.extract_from_list(deepcopy([self.time_book, self.book_bid, self.book_ask]), self.time_push)
                # 直近の記録時間を更新する.
                self.to_MySQL(act, book)
                self.time_push = now
            time.sleep(1.0)


    def to_MySQL(self, act, book):
        """
            ndarrayを受け取りこの関数内でdfに組み上げDBヘpush.
            act   : dict
            book  : [rcv, bid, ask]
        """
        try:
            # DF : bb,ab,quoteの作成
            df_t = pd.DataFrame(act).set_index("rcvTime")
            cols = (book[0].shape[1] / 2) + 1
            col_bid = ["bidPrice"+str(i) for i in range(1,cols)] + ["bidVol"+str(i) for i in range(1,cols)]
            col_ask = ["askPrice"+str(i) for i in range(1,cols)] + ["askVol"+str(i) for i in range(1,cols)]
            try:
                df_bb = pd.DataFrame(book[1], columns=col_bid, index=book[0]);
                df_ab = pd.DataFrame(book[2], columns=col_ask, index=book[0]);
            except:
                df_bb = pd.DataFrame(columns=col_bid)
                df_ab = pd.DataFrame(columns=col_ask)
            engine = create_engine(self.sqlURL, echo=False)
            con = engine.connect()
            df_bb.to_sql("MarketBook_Bid_"+self.symbol, con, if_exists='append')
            df_ab.to_sql("MarketBook_Ask_"+self.symbol, con, if_exists='append')
            df_t.to_sql("MarketActivity_"+self.symbol,  con, if_exists='append')
            con.close()
            engine.dispose()
        except Exception as e:
            if self.logger is not None:
                self.logger.error("BitMEX to_MySQL error: {}".format(e))
            con.close()
            engine.dispose()


    ### OrderBook Methods
    def partial_board(self, message):
        # 板をすげ替える
        book_asknow = SortedDict()
        book_bidnow = SortedDict()
        for m in message["data"]:
            if m["side"] == "Sell":
                book_asknow[m["id"]] = [m["price"], m["size"]]
            elif m["side"] == "Buy":
                book_bidnow[m["id"]] = [m["price"], m["size"]]
        self.book_asknow = book_asknow
        self.book_bidnow = book_bidnow

    def update_board(self, message):
        # Volumeのみ更新
        for m in message["data"]:
            if m["side"] == "Sell":
                self.book_asknow[m["id"]][1] = m["size"]
            elif m["side"] == "Buy":
                self.book_bidnow[m["id"]][1] = m["size"]

    def delete_board(self, message):
        # 消す
        for m in message["data"]:
            if m["side"] == "Sell":
                try: self.book_asknow.pop(m["id"])
                except: pass
            elif m["side"] == "Buy":
                try: self.book_bidnow.pop(m["id"])
                except: pass

    def insert_board(self, message):
        # 挿入
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
        return self.book_bidnow.peekitem(-1)[0]

    @property
    def bestAsk(self):
        return self.book_asknow.peekitem(0)[0]

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