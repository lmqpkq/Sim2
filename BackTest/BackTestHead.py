import SkyNETHead
import pandas as pd
import numpy as np
import os


class CsvDataCenter(SkyNETHead.DataCenter):
    def __init__(self,bwdwin):
        super(CsvDataCenter,self).__init__(bwdwin)

    def Read(self,date,timetick,cache):
        #fieldname = cache["config"]["Field"]
        print "Read Raw Data"
        SkyNETHead.DataManager.LoadRawDataWrapper(SkyNETHead.SkyNETHead.SimpleFilePathGenerator,None,False)(date,timetick,cache)
        print "Read Offset Data"
        SkyNETHead.DataManager.LoadDataPointerWrapper(SkyNETHead.SkyNETHead.SimpleOffsetPathGenerator,False)(date,timetick,cache)

class BTIndicatorManager(SkyNETHead.IndicatorManager):
    def __init__(self,inddict):
        super(BTIndicatorManager,self).__init__()
        self.inddict = inddict
        self.funclist = []

    def AddDependency(self,indid):
        ind = self.inddict[indid]
        if len(ind.depends) <= 0:
            if indid not in self.funclist:
                self.funclist.append(indid)
        else:
            for depindid in ind.depends:
                self.AddDependency(depindid)
            self.funclist.append(indid)

    def GenerateDependency(self,cache):
        for indid in self.inddict.keys():
            self.AddDependency(indid)

    def Compute(self,date,timetick,cache):
        if "Indicator" not in cache.keys():
            cache["Indicator"] = {}
        for indid in self.funclist:
            ind = self.inddict[indid]
            ind.Run(date,timetick,cache)


class BasicDataIndicator(SkyNETHead.Indicator):
    def __init__(self,fieldname,useadj=False):
        super(BasicDataIndicator,self).__init__()
        self.depends = []
        self.fieldname = fieldname
        self.index = []
        self.useadj=useadj

    def Run(self,date,timetick,cache):
        if "BasicData" not in cache.keys():
            cache["BasicData"] = {}
        univ = cache["config"]["Univ"].GetUniv(date)
        fieldname = self.fieldname
        if self.useadj:
            fieldname = self.fieldname+"_adj"
        if timetick==0:
            ind = pd.DatetimeIndex([str(date)+" "+str(t) for t in cache["config"]["TimeTick"]])
            cache["BasicData"][fieldname] = pd.DataFrame(np.nan,index = ind,columns = univ)
        fieldcol = cache["config"]["FieldMap"][self.fieldname]
        for instr in univ:
            offset = cache["Offset"][instr][timetick]
            data = cache["RawData"][instr].iloc[offset,fieldcol]
            if self.useadj:
                try:
                    adj = cache["config"]["AdjFact"].ix[str(date),instr.upper()]
                    data *= adj
                except:
                    data = np.nan
            cache["BasicData"][fieldname].ix[timetick,instr] = data
        return

class OHLCDataIndicator(SkyNETHead.Indicator):
    def __init__(self,fieldname):
        super(OHLCDataIndicator,self).__init__()
        self.depends = []
        self.fieldname = fieldname
        self.index = []

    def Run(self,date,timetick,cache):
        if "BasicData" not in cache.keys():
            cache["BasicData"] = {}
        univ = cache["config"]["Univ"].GetUniv(date)
        if timetick==0:
            ind = pd.DatetimeIndex([str(date)+" "+str(t) for t in cache["config"]["TimeTick"]])
            cache["BasicData"][self.fieldname] = pd.DataFrame(np.nan,index = ind,columns = univ)
        if self.fieldname.startswith("interval_high"):
            if self.fieldname.endswith("adj"):
                self.GetHigh(date,timetick,cache,univ,True)
            else:
                self.GetHigh(date, timetick, cache, univ)
        elif self.fieldname.startswith("interval_low"):
            if self.fieldname.endswith("adj"):
                self.GetLow(date,timetick,cache,univ,True)
            else:
                self.GetLow(date, timetick, cache, univ)
        elif self.fieldname.startswith("interval_open"):
            if self.fieldname.endswith("adj"):
                self.GetOpen(date,timetick,cache,univ,True)
            else:
                self.GetOpen(date, timetick, cache, univ)
        elif self.fieldname.startswith("interval_volume"):
            if self.fieldname.endswith("adj"):
                self.GetVolume(date,timetick,cache,univ,True)
            else:
                self.GetVolume(date, timetick, cache, univ)
        elif self.fieldname.startswith("interval_turnover"):
            self.GetTurnover(date,timetick,cache,univ)
        elif self.fieldname.startswith("interval_vwap"):
            if self.fieldname.endswith("adj"):
                self.GetVwap(date,timetick,cache,univ,True)
            else:
                self.GetVwap(date,timetick,cache,univ)
        return

    def GetHigh(self,date,timetick,cache,univ,useadj=False):
        col = cache["config"]["FieldMap"]["LastPrc"]
        for instr in univ:
            offset = cache["Offset"][instr][timetick]
            try:
                if timetick==0:
                    data = np.nan
                else:
                    previousoffset = cache["Offset"][instr][timetick-1]
                    data = cache["RawData"][instr].iloc[(previousoffset+1):(offset+1),col].max()
                if useadj:
                    data*=self.GetAdj(date,timetick,cache,instr)
                cache["BasicData"][self.fieldname].ix[timetick,instr] = data
            except:
                print "High",instr,date,timetick,offset
        return

    def GetLow(self,date,timetick,cache,univ,useadj=False):
        col = cache["config"]["FieldMap"]["LastPrc"]
        for instr in univ:
            offset = cache["Offset"][instr][timetick]
            try:
                if timetick==0:
                    data = np.nan
                else:
                    previousoffset = cache["Offset"][instr][timetick-1]
                    data = cache["RawData"][instr].iloc[(previousoffset+1):(offset+1),col].min()
                if useadj:
                    data*=self.GetAdj(date,timetick,cache,instr)
                cache["BasicData"][self.fieldname].ix[timetick,instr] = data
            except:
                print "Low",instr,date,timetick,offset
        return

    def GetTurnover(self,date,timetick,cache,univ):
        col = cache["config"]["FieldMap"]["TurnoverD"]
        for instr in univ:
            offset = cache["Offset"][instr][timetick]
            try:
                if timetick==0:
                    data = cache["RawData"][instr].iloc[offset,col].sum()
                else:
                    previousoffset = cache["Offset"][instr][timetick-1]
                    data = cache["RawData"][instr].iloc[:(offset+1),col].sum()-cache["RawData"][instr].iloc[:(previousoffset+1),col].sum()
                cache["BasicData"][self.fieldname].ix[timetick,instr] = data
            except:
                print "Turnover",instr,date,timetick,offset
        return

    def GetVolume(self,date,timetick,cache,univ,useadj=False):
        col = cache["config"]["FieldMap"]["TurnoverS"]
        for instr in univ:
            offset = cache["Offset"][instr][timetick]
            try:
                if timetick==0:
                    data = cache["RawData"][instr].iloc[offset,col].sum()
                else:
                    previousoffset = cache["Offset"][instr][timetick-1]
                    data = cache["RawData"][instr].iloc[:(offset+1),col].sum()-cache["RawData"][instr].iloc[:(previousoffset+1),col].sum()
                if useadj:
                    data = data*1.0/self.GetAdj(date,timetick,cache,instr)
                cache["BasicData"][self.fieldname].ix[timetick,instr] = data
            except:
                print "Volume",instr,date,timetick,offset
        return

    def GetOpen(self,date,timetick,cache,univ,useadj=False):
        col = cache["config"]["FieldMap"]["LastPrc"]
        for instr in univ:
            offset = cache["Offset"][instr][timetick]
            try:
                if timetick==0:
                    data = cache["RawData"][instr].iloc[offset,col]
                else:
                    previousoffset = cache["Offset"][instr][timetick-1]
                    data = cache["RawData"][instr].iloc[previousoffset,col]
                if useadj:
                    data*=self.GetAdj(date,timetick,cache,instr)
                cache["BasicData"][self.fieldname].ix[timetick,instr] = data
            except:
                print "Open",instr,date,timetick,offset
        return

    def GetVwap(self,date,timetick,cache,univ,useadj=False):
        col1 = cache["config"]["FieldMap"]["TurnoverD"]
        col2 = cache["config"]["FieldMap"]["TurnoverS"]
        for instr in univ:
            offset = cache["Offset"][instr][timetick]
            try:
                data = np.nan
                if timetick == 0:
                    to = cache["RawData"][instr].iloc[offset, col1].sum()
                    vol = cache["RawData"][instr].iloc[offset, col2].sum()
                    if vol>0:
                        data = to/vol
                else:
                    previousoffset = cache["Offset"][instr][timetick - 1]
                    to = cache["RawData"][instr].iloc[:(offset+1), col1].sum() - cache["RawData"][instr].iloc[
                                                                              :(previousoffset+1), col1].sum()
                    vol = cache["RawData"][instr].iloc[:(offset+1), col2].sum() - cache["RawData"][instr].iloc[
                                                                              :(previousoffset+1), col2].sum()
                    if vol>0:
                        data = to/vol
                if useadj:
                    data *= self.GetAdj(date, timetick, cache, instr)
                cache["BasicData"][self.fieldname].ix[timetick, instr] = data
            except Exception,e:
                print e
        return

    def GetAdj(self,date,timetick,cache,instr):
        return cache["config"]["AdjFact"].ix[str(date),instr]

class MoneyFluxIndicator(SkyNETHead.Indicator):
    def __init__(self,fieldname):
        super(MoneyFluxIndicator,self).__init__()
        self.depends = []
        self.fieldname = fieldname
        self.index = []

    def Run(self,date,timetick,cache):
        if "BasicData" not in cache.keys():
            cache["BasicData"] = {}
        univ = cache["config"]["Univ"].GetUniv(date)
        if timetick==0:
            ind = pd.DatetimeIndex([str(date)+" "+str(t) for t in cache["config"]["TimeTick"]])
            cache["BasicData"][self.fieldname] = pd.DataFrame(np.nan,index = ind,columns = univ)
        param = self.fieldname.split('_')
        t = param[0]
        ds = param[1]
        bs = param[2]
        self.Get(date,timetick,cache,univ,t,ds,bs)
        return

    def Get(self,date,timetick,cache,univ,t="ExB",ds="v",bs="b"):
        upbound= 1000
        downbound = 500
        volumeshareCol = cache["config"]["FieldMap"]["TurnoverS"]
        dirCol = cache["config"]["FieldMap"]["Dir"]
        if ds=="v":
            col = cache["config"]["FieldMap"]["TurnoverS"]
        elif ds=="d":
            col = cache["config"]["FieldMap"]["TurnoverD"]
        if t=="ExB":
            upbound = +np.inf
            downbound = 1000
        elif t=="B":
            upbound = 1000
            downbound = 500
        elif t=="M":
            upbound = 500
            downbound = 50
        elif t=="S":
            upbound = 50
            downbound = 0
        for instr in univ:
            offset = cache["Offset"][instr][timetick]
            try:
                if timetick==0:
                    if cache["RawData"][instr].iloc[offset,dirCol].lower()==bs:
                        if (cache["RawData"][instr].iloc[offset,volumeshareCol]>downbound)&(cache["RawData"][instr].iloc[offset,volumeshareCol]<=upbound):
                            date = cache["RawData"][instr].iloc[offset,col]
                    else:
                        data = 0
                else:
                    previousoffset = cache["Offset"][instr][timetick-1]
                    alldata = cache["RawData"][instr].iloc[(previousoffset+1):(offset+1),:]
                    #print alldata
                    data = alldata[(alldata.iloc[:,volumeshareCol]>downbound)&(alldata.iloc[:,volumeshareCol]<=upbound)&(alldata.iloc[:,dirCol]==bs.upper())].iloc[:,col].sum()
                cache["BasicData"][self.fieldname].ix[timetick,instr] = data
            except:
                print "MoneyFlux",instr,date,timetick,offset
        return

    def GetAdj(self,date,timetick,cache,instr):
        return cache["config"]["AdjFact"].ix[str(date),instr]




class DumpDataInidcator(SkyNETHead.Indicator):
    def __init__(self,fieldlist,targetpath):
        super(DumpDataInidcator,self).__init__()
        self.depends = fieldlist
        self.targetpath = targetpath
        self.fieldlist = fieldlist

    def Run(self,date,timetick,cache):
        if "tmp" not in cache.keys():
            cache["tmp"] = dict()
        if (date==cache["DateList"][0])&(timetick==cache["config"]["SpotPerDay"]-1):
            print "First day"
            for field in self.fieldlist:
                cache["tmp"][field] = cache["BasicData"][field]
        elif (timetick==cache["config"]["SpotPerDay"]-1):
            print "Concate data"
            for field in self.fieldlist:
                cache["tmp"][field] = pd.concat([cache["tmp"][field],cache["BasicData"][field]],axis=0)
        if (date == cache["DateList"][-1])&(timetick==cache["config"]["SpotPerDay"]-1):
            for field in self.fieldlist:
                cache["tmp"][field].to_pickle(os.path.join(self.targetpath,field+".pic"))