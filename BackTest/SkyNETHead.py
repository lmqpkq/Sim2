import pandas as pd
import os
import numpy as np
import datetime as dt
import cPickle
from collections import OrderedDict
from abc import *
from multiprocessing import Process
import threading
import logging


LOCK1 = threading.Lock()
MIN_UNIT = 1e-8


def ReadFile(date,timetick,cache,instr,filepathgenerator,fieldname):
    filename = filepathgenerator(date, timetick, cache, instr)
    data = pd.read_csv(filename, encoding="gbk")
    if fieldname is not None:
        data.columns = fieldname
    if LOCK1.acquire():
        cache["RawData"][instr] = data
        LOCK1.release()


class SkyNETHead:
    @staticmethod
    def SimpleFilePathGenerator(date,timetick,cache,instr):
        if os.path.exists(os.path.join(cache["config"]["DataSource"],date[0:4],date,instr+"_"+date+".csv")):
            return os.path.join(cache["config"]["DataSource"],date[0:4],date,instr+"_"+date+".csv")
        elif os.path.exists(os.path.join(cache["config"]["DataSource"],date[0:4],date,instr.lower()+"_"+date+".csv")):
            return os.path.join(cache["config"]["DataSource"],date[0:4],date,instr.lower()+"_"+date+".csv")
        else:
            #return None
            raise Exception("[Warning]:"+date+" Missing instrument "+instr)

    @staticmethod
    def SimpleOffsetPathGenerator(date,timetick,cache,instr):
        if os.path.exists(os.path.join(cache["config"]["OffsetSource"],date,instr+".csv")):
            return os.path.join(cache["config"]["OffsetSource"],date,instr+".csv")
        elif os.path.exists(os.path.join(cache["config"]["OffsetSource"],date,instr.lower()+".csv")):
            return os.path.join(cache["config"]["OffsetSource"],date,instr.lower()+".csv")
        else:
            #return None
            raise Exception("[Warning]:"+date+" Missing instrument "+instr)


class WorkFlowManager(object):
    def __init__(self):
        self.PreFunclist = []
        self.SODFunclist = []
        self.EODFunclist = []
        self.TickFunclist = []
        self.PostFunclist = []

    def AddPre(self,funclist):
        self.PreFunclist+=funclist

    def AddPost(self,funclist):
        self.PostFunclist += funclist

    def AddSOD(self,funclist):
        self.SODFunclist += funclist

    def AddTICK(self,funclist):
        self.TickFunclist += funclist

    def AddEOD(self,funclist):
        self.EODFunclist += funclist

    def GetPre(self):
        return self.PreFunclist

    def GetPost(self):
        return self.PostFunclist

    def GetSOD(self):
        return self.SODFunclist

    def GetEOD(self):
        return self.EODFunclist

    def GetTICK(self):
        return self.TickFunclist

class BackTestWorkFlow(WorkFlowManager):
    def __init__(self):
        super(BackTestWorkFlow,self).__init__()
        return

    def Init(self,datacenter,indicatormanager,alphamanger,combinemanager,trademanager,ordermanager):
        self.AddSOD([datacenter.Read])
        self.AddTICK([indicatormanager.Compute,
                     alphamanger.Compute,
                     combinemanager.Compute,
                     trademanager.GenOrder,
                     ordermanager.SendOrder,
                     ])
        self.AddEOD([datacenter.Store,
                     ordermanager.ComputePNL])

class DataCenter(object):
    def __init__(self,bwdwin):
        self.TickData = {}
        self.DailyData = {}
        self.ExtraData = {}
        self.HistTickData = OrderedDict()
        self.HistDailyData = OrderedDict()
        self.HistExtraData = OrderedDict()
        self.BwdWin = bwdwin
        return

    @abstractmethod
    def Read(self,date,timetick,cache):
        return

    def Store(self,date,timetick,cache):
        self.HistDailyData[date] = self.DailyData
        self.HistTickData[date] = self.TickData
        self.HistExtraData[date] = self.ExtraData
        if len(self.HistExtraData.keys())>self.BwdWin:
            deletdate = self.HistExtraData.keys()[0]
            del self.HistTickData[deletdate]
            del self.HistDailyData[deletdate]
            del self.HistExtraData[deletdate]
        return

class IndicatorManager(object):
    def __init__(self):
        return

    def Compute(self,date,timetick,cache):
        return

class AlphaManager(object):
    def __init__(self):
        return

    def Compute(self,date,timetick,cache):
        return

class CombineManager(object):
    def __init__(self):
        return

    def Compute(self,date,timetick,cache):
        return

class TradeManager(object):
    def __init__(self):
        return

    def GenOrder(self,date,timetick,cache):
        return

class OrderManager(object):
    def __init__(self):
        return

    def SendOrder(self,date,timetick,cache):
        return

    def ComputePNL(self,date,timetick,cache):
        return

class Indicator(object):
    def __init__(self):

        self.depends = []
        return

    def Run(self,date,timetick,cache):
        return

class DataManager:
    @staticmethod
    def LoadRawDataWrapper(filepathgenerator,fieldname = None,p = True):
        def LoadData(date,timetick,cache):
            start= dt.datetime.now()
            for instr in cache["config"]["Univ"].GetUniv(date):
                if p:
                    print "Load",instr,"On",date
                try:
                    filepath = filepathgenerator(date,timetick,cache,instr)
                except Exception,ex:
                    pass
                try:
                    data = pd.read_csv(filepath,encoding="gbk")
                    if fieldname is not None:
                        data.columns = fieldname
                    cache["RawData"][instr] = data.copy(deep=True)
                    logging.info(filepath+" Generated")
                except:
                    logging.error("Missing Data file/Empty file "+instr+" "+str(date))
            print "All Data Loaded:",dt.datetime.now()-start,"second"
        return LoadData

    @staticmethod
    def LoadRawDataMPWrapper(filepathgenerator,fieldname = None,p = True):

        def LoadData(date,timetick,cache):
            processlist = []
            for instr in cache["config"]["Univ"].GetUniv(date):
                p = Process(target=ReadFile,args = (date,timetick,cache,instr,filepathgenerator,fieldname))
                processlist.append(p)
            for p in processlist:
                p.start()
                p.join()


        return LoadData

    @staticmethod
    def LoadDataPointerWrapper(filepathgenerator,p=True):
        def LoadPointer(date,timetick,cache):
            for instr in cache["config"]["Univ"].GetUniv(date):
                if p:
                    print "Load Pointer",instr,"On",date
                try:
                    filepath = filepathgenerator(date,timetick,cache,instr)
                except Exception,ex:
                    pass
                try:
                    data = list(pd.read_csv(filepath,header=None).T.values[0])
                    cache["Offset"][instr] = data
                    logging.info(filepath + " Generated")
                except:
                    logging.error("Missing Offset file/Empty file "+instr+" "+str(date))
        return LoadPointer

    @staticmethod
    def ExtractIntervalDataWrapper(targetfieldname,op):
        def Extract(date,timetick,cache):
            assert targetfieldname not in cache["Data"].keys()
            cache["Data"][targetfieldname] = pd.DataFrame(np.nan,index = range(cache["config"]["SpotPerDay"]),columns = cache["config"]["Univ"])
            for instr in cache["config"]["Univ"].GetUniv(date):
                data = cache["RawData"][instr]
                cache["Data"][targetfieldname].ix[:,instr]=op(data,cache)
        return Extract

    @staticmethod
    def ExtractSpotDataWrapper(fieldname,spotlist):
        def Extract(date,timetick,cache):
            assert fieldname not in cache["Data"].keys()
            cache["Data"][fieldname] = pd.DataFrame(np.nan,index=spotlist,columns = cache["config"]["Univ"])
            for instr in cache["config"]["Univ"].GetUniv(date):
                data = cache["RawData"][instr]
                cache["Data"][fieldname].ix[:,instr] = data.ix[spotlist,fieldname]
        return Extract

    @staticmethod
    def SpotListGenerator():
        def Gen(date,timetick,cache):
            start = dt.datetime.now()
            univ = cache["config"]["Univ"].GetUniv(date)
            for instr in univ:
                #print instr
                offset = []
                pointer1 = 0
                pointer2 = 0
                try:
                    data = cache["RawData"][instr]
                    if instr =="SZ300228":
                        print data
                except:
                    break
                while (pointer2<cache["config"]["SpotPerDay"])&(pointer1<data.shape[0]):
                    date1 = dt.datetime.strptime(data.ix[pointer1,"DateTime"],"%Y-%m-%d %H:%M:%S")
                    date2 = dt.datetime.strptime(date+" "+cache["config"]["TimeTick"][pointer2],"%Y%m%d %H:%M:%S")
                    if (date1<=date2):
                        pointer1+=1
                    else:
                        if pointer1 <= 0:
                            offset.append(pointer1)
                            pointer2+=1
                        else:
                            offset.append(pointer1-1)
                            pointer2+=1
                for i in range(len(cache["config"]["TimeTick"])-len(offset)):
                    offset.append(pointer1-1)
                if not (len(offset)==len(cache["config"]["TimeTick"])):
                    print instr
                    raise Exception("Error: Wrong Offset Length")
                cache["Offset"][instr] = pd.Series(offset)
            print "Offset Generated:",dt.datetime.now()-start,"second"
        return Gen

    @staticmethod
    def DumpCacheDataWrapper(dataname,filenamegenerator,byinstr=True):
        def DumpCache(date,timetick,cache):
            if byinstr:
                for instr in cache["config"]["Univ"].GetUniv(date):
                    filename = filenamegenerator(date,timetick,cache,instr)
                    dirname = os.path.dirname(filename)
                    if not os.path.exists(dirname):
                        os.makedirs(dirname)
                    try:
                        cache[dataname][instr].to_csv(filename,index=False)
                    except:
                        break
        return DumpCache

class Univ:
    def __init__(self,sourcepath):
        self.__sourcepath__ = sourcepath

    def GetUniv(self,date):
        fpath = os.path.join(self.__sourcepath__,str(date)+".csv")
        univ = list(pd.read_csv(fpath,header=None).T.values[0])
        return univ



class Order:
    def __init__(self,dailyClear=False):
        self.totalRecort = {}
        self._orderDataList = []
        self._dateList = []
        self._dailyClear = dailyClear

        self.dailyRecord = {}
        self.position = {}
        self.buy_trade = {}
        self.sell_trade = {}

        self.id2Order = {}
        self.activeID = []

    def initPosition(self,initOrder):
        print "om do nothing", initOrder

    def sendOrder(self,order,fakeFill = True,algoFunc = None,params = {}):
        order["real"] = 1

        ticker = order["ticker"]
        if ticker not in self.dailyRecord:
            self.dailyRecord[ticker] = []

        self.dailyRecord[ticker].append(order)

        if fakeFill:
            order["fVolume"] = order["volume"]
            order["fPrice"] = order["price"]

            if ticker not in self.position:
                self.position[ticker] = 0
            self.position[ticker] += order["fVolume"]

            if order["volume"]>0:
                data = self.buy_trade.setdefault(ticker,np.zeros(3))
                data[0] += order["fVolume"]
                data[1] += order["fVolume"] * order["fPrice"]

            elif order["volume"] <0:
                data = self.sell_trade.setdefault(ticker,np.zeros(3))
                data[0] -= order["fVolume"]
                data[1] -= order["fVolume"] * order["fPrice"]

            data[2] += order["fVolume"]*(order["fPrice"]- order["bPrice"])

        if algoFunc is not None:
            algoFunc(orderManager = self,order = order,**params)

    def dailySettleWrapper(self,settleFunction = lambda ticker,date,timetick,cache:np.nan,close=False,closeList = []):
        def dailySettle(date,timetick,cache):
            self.totalRecort[date] = {}
            newDailyRecord = {}
            newOrderList = []

            for ticker in self.dailyRecord:
                closeFlag = close if ticker not in closeList else True

                volume = -sum([i["fVolume"] for i in self.dailyRecord[ticker]])

                if abs(float(volume)) > MIN_UNIT:
                    settlePrice = settleFunction(ticker,date,timetick,cache)
                    settlePrice = settlePrice if not np.nan else self.dailyRecord[ticker][-1]["price"]
                    settleOrder = {"ticker":ticker,
                                   "volume":volume,
                                   "price":settlePrice,
                                   "fVolume":volume,
                                   "fPrice":settlePrice,
                                   "date":date,
                                   "timetick":-2,
                                   "real":1 if closeFlag else 0,
                                   }
                    self.dailyRecord[ticker].append(settleOrder.copy())

                if self._dailyClear:
                    for o in self.dailyRecord[ticker]:
                        newOrderList.append(o)
                else:
                    self.totalRecort[date][ticker] = self.dailyRecord[ticker]

                if abs(float(volume))>MIN_UNIT:
                    if not closeFlag:
                        settleOrder["volume"] = -settleOrder["volume"]
                        settleOrder["fVolume"] = -settleOrder["fVolume"]
                        settleOrder["timetick"] = -1
                        newDailyRecord[ticker] = [settleOrder]
                    else:
                        self.position[ticker] = 0

            if self._dailyClear:
                newData = pd.DataFrame(newOrderList)
                newData["orderRecordDate"] = date
                self._orderDataList.append(newData)

            self.dailyRecord = newDailyRecord
            self.buy_trade = {}
            self.sell_trade = {}

        return dailySettle

    class Order:
        def __init__(self,ticker,volume,price,date,timetick):
            self.ticker = ticker
            self.volume = volume
            self.price = price
            self.date = date
            self.timetick = timetick

"""
class IndicatorManager:
    def __init__(self,indlist,bwdday=20):
        self.inddict = {ind.indID:ind for ind in indlist}
        self.record = OrderedDict() #record by date
        self.curr = {} #record by indicator name and then instrument
        self.bwdday = bwdday
        self.recordday = 0
        self._generationorderlist_ = self.GetGenerationOrderlist()

    def RecordEOD(self,date,timetick,cache):
        self.record[date] = self.curr
        self.recordday+=1
        if self.recordday>self.bwdday:
            del self.recordday[self.recordday.keys()[0]]
            self.recordday -= 1

    def initDaily(self,date,timetick,cache):
        self.curr = {}
        for indname in self.inddict.keys():
            self.curr[indname] = {}
            for instr in cache["config"]['Univ'].GetUniv(date):
                self.curr[indname][instr] = []

    def GenSODFunclist(self,date,timetick,cache):
        output = []
        for indname in self._generationorderlist_:
            output.append(self.inddict[indname].SODFunc)
        return output

    def GenTickFunclist(self,date,timetick,cache):
        output = []
        for indname in self._generationorderlist_:
            output.append(self.inddict[indname].TickFunc)
        return output

    def GenEODFunclist(self,date,timetick,cache):
        output = []
        for indname in self._generationorderlist_:
            output.append(self.inddict[indname].EODFunc)
        return output

    def GetGenerationOrderlist(self):
        l = []
        for indname in self.inddict.keys():
            Genlist = self.GenerationFromSingle(indname)
            for dep in Genlist:
                if dep not in l:
                    l.append(dep)
        return l

    def GenerationFromSingle(self,indname):
        if len(self.inddict[indname].depends)==0:
            return [indname]
        else:
            deplist = self.inddict[indname].depends
            output = []
            for dep in deplist:
                output += self.GenerationFromSingle(dep)
            output.append(indname)
            return output
"""


