import pandas as pd
from collections import OrderedDict
import os
import datetime

class SkyNETSim:
    def __init__(self,config):
        self.__start__ = config["Start"]
        self.__end__ = config["End"]
        self.__datelist__ = []
        self.__spotPerDay__ = config["SpotPerDay"]
        self.__preBTfunclist__ = config["preBTfunc"]
        self.__postBTfunclist__ = config["postBTfunc"]
        self.__tickfunclist__ = config["tickFunc"]
        self.__sodfunclist__ = config["sodFunc"]
        self.__eodfunclist__ = config["eodFunc"]
        self.__cache__ = dict()
        self.__cache__['config'] = config
        self.__cache__["RawData"] = dict()
        self.__cache__["Offset"] = dict()

    def Run(self):
        self.__prebacktest__()
        self.__backtest__()
        self.__postbacktest__()

    def __prebacktest__(self):
        #Prepare backtest business day
        print self.__end__,self.__start__
        for bday in self.__cache__['config']['BDay']:
            if (bday<=self.__end__)&(bday>=self.__start__):
                self.__datelist__.append(bday)
        self.__cache__["DateList"] = self.__datelist__
        for func in self.__preBTfunclist__:
            func(self.__cache__)
        return

    def __backtest__(self):
        for date in self.__datelist__:
            print date
            for timetick in range(self.__spotPerDay__):
                timestart = datetime.datetime.now()
                print date, timetick,"Start Loop"
                if timetick==0:
                    for func in self.__sodfunclist__:
                        func(date,timetick,self.__cache__)
                    for func in self.__tickfunclist__:
                        func(date,timetick,self.__cache__)
                elif timetick==self.__spotPerDay__-1:
                    for func in self.__eodfunclist__:
                        func(date,timetick,self.__cache__)
                    for func in self.__tickfunclist__:
                        func(date,timetick,self.__cache__)
                else:
                    for func in self.__tickfunclist__:
                        func(date,timetick,self.__cache__)
                print date,timetick,"End Loop:",datetime.datetime.now()-timestart
        return

    def __postbacktest__(self):
        for func in self.__postBTfunclist__:
            func(self.__cache__)
        return


