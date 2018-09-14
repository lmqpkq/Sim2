import pandas as pd
import SkyNETHead
import os
import logging
import BackTestHead

BDayPath = "E:\JinShuYuan\Misc\BDay.csv"
Start = "20110104"
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
adjfact = pd.read_pickle("E:/JinShuYuan/PdData/DailyData/AdjFact.pic")
fieldMap = {fieldName[i]:i for i in range(len(fieldName))}
bwdwin = 60


inddict = {"ExB_v_b":BackTestHead.MoneyFluxIndicator("ExB_v_b"),
           #"ExB_v_s":BackTestHead.MoneyFluxIndicator("ExB_v_s"),
           #"ExB_d_b":BackTestHead.MoneyFluxIndicator("ExB_d_b"),
           #"ExB_d_s":BackTestHead.MoneyFluxIndicator("ExB_d_s"),
           #"B_v_b":BackTestHead.MoneyFluxIndicator("B_v_b"),
           #"B_v_s":BackTestHead.MoneyFluxIndicator("B_v_s"),
           #"B_d_b":BackTestHead.MoneyFluxIndicator("B_d_b"),
           #"B_d_s":BackTestHead.MoneyFluxIndicator("B_d_s"),
           #"M_v_b":BackTestHead.MoneyFluxIndicator("M_v_b"),
           #"M_v_s":BackTestHead.MoneyFluxIndicator("M_v_s"),
           #"M_d_b":BackTestHead.MoneyFluxIndicator("M_d_b"),
           #"M_d_s":BackTestHead.MoneyFluxIndicator("M_d_s"),
           #"S_v_b":BackTestHead.MoneyFluxIndicator("S_v_b"),
           #"S_v_s":BackTestHead.MoneyFluxIndicator("S_v_s"),
           #"S_d_b":BackTestHead.MoneyFluxIndicator("S_d_b"),
           #"S_d_s":BackTestHead.MoneyFluxIndicator("S_d_s"),
           #"Dump":BackTestHead.DumpDataInidcator(["ExB_v_b","ExB_v_s","ExB_d_b","ExB_d_s","B_v_b","B_v_s","B_d_b","B_d_s","M_v_b","M_v_s","M_d_b","M_d_s","S_v_b","S_v_s","S_d_b","S_d_s",
           #                                       ],"J:/ResearchData/Stock/5min/JSY3000"),
           "Dump":BackTestHead.DumpDataInidcator(["ExB_v_b"],"J:/ResearchData/Stock/5min/JSY3000/"),
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