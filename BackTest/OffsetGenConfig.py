import pandas as pd
import SkyNETHead
import os
import logging

BDayPath = "E:\JinShuYuan\Misc\BDay.csv"
Start = "20070104"
End = "20170104"
BDay = sorted([str(d) for d in list(pd.read_csv(BDayPath,header=None).T.values[0])])
DataSource = "D:\StockData\TickData"
OffsetSource = "E:\JinShuYuan\Misc\Offset/5min"
Univ = SkyNETHead.Univ("E:/JinShuYuan/Misc/Univ/JSY3000")
timetick = list(pd.read_csv("E:/JinShuYuan/Misc/TimeTick/TimeTick_5min.csv",header=None).T.values[0])
OffsetPath = "E:/JinShuYuan/Misc/Offset"
fieldName = ["Mkt","Code","DateTime","LastPrc","TradeNum","TurnoverD","TurnoverS","Dir","Bid1","Bid2","Bid3","Bid4",
             "Bid5","Ask1","Ask2","Ask3","Ask4","Ask5","bsize1","bsize2","bsize3","bsize4","bsize5","asize1","asize2",
             "asize3","asize4","asize5"]
fieldMap = {fieldName[i]:i for i in range(len(fieldName))}
logging.basicConfig(filename="E:\JinShuYuan\Misc\Offset/Gen.log",level=logging.ERROR)

offsetconfig = {"BDay":BDay,
              "Start":Start,
              "End":End,
              "TimeTick":timetick,
              "SpotPerDay":len(timetick),
              "DataSource":DataSource,
              "Univ":Univ,
              "preBTfunc":[],
              "postBTfunc":[],
              "tickFunc":[],
              "sodFunc":[SkyNETHead.DataManager.LoadRawDataWrapper(SkyNETHead.SkyNETHead.SimpleFilePathGenerator,fieldName),
                         SkyNETHead.DataManager.SpotListGenerator(),
                         ],
              "eodFunc":[SkyNETHead.DataManager.DumpCacheDataWrapper("Offset",lambda date,timetick,cache,instr:os.path.join(OffsetPath,"5min",date,instr+".csv")),
                         ],
              }

