import pandas as pd
import sys
sys.path.append("F:/Entertainment/SkyNET/BackTest")
import SkyNETHead
import BackTestHead
import SkyNETSim
import os

BDayPath = "E:\JinShuYuan\Misc\BDay.csv"
Start = "20110101"
End = "20171231"
BDay = sorted([str(d) for d in list(pd.read_csv(BDayPath,header=None).T.values[0])])
DataSource = "D:\StockData\TickData"
OffsetSource = "E:/JinShuYuan/Misc/Offset/5min"
Univ = SkyNETHead.Univ("E:/JinShuYuan/Misc/Univ/JSY3000")
timetick = list(pd.read_csv("E:/JinShuYuan/Misc/TimeTick/TimeTick_5min.csv",header=None).T.values[0])
OffsetPath = "E:/JinShuYuan/Misc/Offset"
fieldName = ["Mkt","Code","DateTime","LastPrc","TradeNum","TurnoverD","TurnoverS","Dir","Bid1","Bid2","Bid3","Bid4",
             "Bid5","Ask1","Ask2","Ask3","Ask4","Ask5","bsize1","bsize2","bsize3","bsize4","bsize5","asize1","asize2",
             "asize3","asize4","asize5"]
adjfact = pd.read_pickle("J:/PdData/AdjFact.pic")
fieldMap = {fieldName[i]:i for i in range(len(fieldName))}
bwdwin = 60


inddict = {"LastPrc_adj":BackTestHead.BasicDataIndicator("LastPrc",True),
           #"LastPrc":BackTestHead.BasicDataIndicator("LastPrc",False),
           #"interval_open_adj":BackTestHead.OHLCDataIndicator("interval_open_adj"),
           #"interval_high_adj":BackTestHead.OHLCDataIndicator("interval_high_adj"),
           #"interval_low_adj":BackTestHead.OHLCDataIndicator("interval_low_adj"),
           #"interval_volume_adj":BackTestHead.OHLCDataIndicator("interval_volume_adj"),
           #"interval_vwap_adj":BackTestHead.OHLCDataIndicator("interval_vwap_adj"),
           #"interval_turnover":BackTestHead.OHLCDataIndicator("interval_turnover"),
           #"Dump":BackTestHead.DumpDataInidcator(["LastPrc_adj","LastPrc","interval_open_adj","interval_high_adj","interval_low_adj",
                                                  #"interval_vwap_adj","interval_turnover"],"J:/ResearchData/Stock/5min/JSY3000"),
           "Dump":BackTestHead.DumpDataInidcator(["LastPrc_adj"],"J:/ResearchData/Stock/5min/JSY3000"),
           }

DC = BackTestHead.CsvDataCenter(bwdwin)
IM = BackTestHead.BTIndicatorManager(inddict)
AM = SkyNETHead.AlphaManager()
CM = SkyNETHead.CombineManager()
TM = SkyNETHead.TradeManager()
OM = SkyNETHead.OrderManager()

BackTestWorkflow = SkyNETHead.BackTestWorkFlow()
BackTestWorkflow.Init(DC,IM,AM,CM,TM,OM)

backtestconfig = {"BDay":BDay,
                  "Start":Start,
                  "End":End,
                  "TimeTick":timetick,
                  "SpotPerDay":len(timetick),
                  "DataSource":DataSource,
                  "OffsetSource":OffsetSource,
                  "Field":fieldName,
                  "FieldMap":fieldMap,
                  "AdjFact":adjfact,
                  "Univ":Univ,
                  "OM":OM,
                  "IM":IM,
                  "preBTfunc":BackTestWorkflow.GetPre()+[IM.GenerateDependency],
                  "postBTfunc":BackTestWorkflow.GetPost(),
                  "tickFunc":BackTestWorkflow.GetTICK(),
                  "sodFunc":BackTestWorkflow.GetSOD(),
                  "eodFunc":BackTestWorkflow.GetEOD(),

                  }

sim = SkyNETSim.SkyNETSim(backtestconfig)

sim.Run()