import sys,os
import datetime,time
import sqlalchemy as sa
from sqlalchemy import create_engine, MetaData, Column
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.mysql import DATETIME, DOUBLE, FLOAT, INTEGER, TEXT, JSON, VARCHAR, BIGINT

from ..conf.mysqlconf import URL
LEVEL = 20
DBNAME = "bitFlyer"
BASE = declarative_base()


class market_activity(BASE):
    __tablename__ = "market_activity"
    id = Column(BIGINT(unsigned=True), nullable=False, primary_key=True)
    symbol = Column(VARCHAR(12), nullable=False, primary_key=True)
    rcvTime = Column(DATETIME(fsp=3), nullable=False)
    mktTime = Column(DATETIME(fsp=3), nullable=False)
    side = Column(VARCHAR(10), nullable=False)
    price = Column(FLOAT(), nullable=False)
    amount = Column(FLOAT(), nullable=False)
    buy_order_id = Column(VARCHAR(30))
    sell_order_id = Column(VARCHAR(30))


class lv1(BASE):
    __tablename__ = "lv1"
    id = Column(BIGINT(unsigned=True), nullable=False, primary_key=True)
    symbol = Column(VARCHAR(12), primary_key=True)
    rcvTime = Column(DATETIME(fsp=3))
    bestBid = Column(FLOAT())
    bestAsk = Column(FLOAT())
    midPrice = Column(FLOAT())


class market_book(BASE):
    __tablename__ = "market_book"
    symbol = Column(VARCHAR(12), primary_key=True)
    rcvTime = Column(DATETIME(fsp=3), nullable=False, primary_key=True)
for i in range(1, LEVEL+1):
    setattr(market_book, f"bidPrice{i}", Column(FLOAT()))
    setattr(market_book, f"bidVol{i}", Column(FLOAT()))
    setattr(market_book, f"askPrice{i}", Column(FLOAT()))
    setattr(market_book, f"askVol{i}", Column(FLOAT()))


def create():
    # Databaseの作成
    try:
        engine = create_engine(URL, encoding='utf-8')
        con = engine.connect()
        con.execute(f"create database {DBNAME}")
    except:
        pass
    finally:
        try:
            con.close()
            engine.dispose()
        except:
            pass

    # tableの作成    
    metadata = MetaData()
    engine = create_engine(URL+DBNAME, encoding='utf-8')
    session = scoped_session(sessionmaker(autocommit=False,
                                          autoflush=True,
                                          expire_on_commit=False,
                                          bind=engine))
    try:
        BASE.metadata.create_all(bind=engine)
    except:
        pass
    finally:
        session.close()
        engine.dispose()


