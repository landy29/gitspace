import random
import numpy as np
import pandas
import pandas as pd
from WindPy import w
import warnings
warnings.filterwarnings('ignore')
w.start()
# 导入当天日期
import time
file_time = time.strftime('%Y%m%d',time.localtime(time.time()))

#======================================================================================================================
'''
第一张表，母行上清持仓表
'''
def my_sheet1(PATH_DATA,DATA):
    TEMP_df1_1 = pd.read_excel(PATH_DATA+"上清"+DATA+"历史持仓余额1.xlsx",thousands=",",converters={"持有人账号": str, "债券代码": str}, index_col=0, header=0)
    TEMP_df1_2 = pd.read_excel(PATH_DATA+"上清"+DATA+"历史持仓余额2.xlsx",thousands=",",converters={"持有人账号": str, "债券代码": str}, index_col=0, header=0)
    TEMP_df1_3 = pd.read_excel(PATH_DATA+DATA+"迁移产品上清历史持仓余额.xlsx",thousands=",",converters={"持有人账号": str, "债券代码": str}, index_col=0, header=0)
    df01 = pd.concat([TEMP_df1_1, TEMP_df1_2, TEMP_df1_3])
    df01.columns = ["证券托管户账号", "持有人账户简称", "债券代码", "债券名称", "ISIN编码", "科目代码", "科目名称", "余额方向", "余额（元）", "月末标识", "更新时间"]
    bond = df01["债券代码"].tolist()
    bonds = [i + ".IB" for i in bond]
    mybonds_list = []
    for i in bonds:
        temp_df = w.wsd(i,"maturitydate","ED-0D", REPO_DATA,"",usedf=True)[1]
        mybonds_list.append(temp_df)
    df_bond_info = pd.concat(mybonds_list)
    df_bond_info.index = range(len(df_bond_info))
    df02=pd.concat([df01,df_bond_info],axis=1)
    df02.columns = ["证券托管户账号", "持有人账户简称", "债券代码", "债券名称", "ISIN编码", "科目代码", "科目名称", "余额方向", "余额（元）", "月末标识", "更新时间","到期日"]
    return df02
'''
第二张表，中债持仓表
'''
def my_sheet2(PATA_DATA,DATA):
    TEMP_df2_1 = pd.read_excel(PATA_DATA+DATA+"迁移产品中债总对账单.xlsx",thousands=",",converters={"债券账号": str, "债券代码": str}, header=0)
    TEMP_df2_2 = pd.read_excel(PATA_DATA+"中债"+DATA+"总对账单.xlsx",thousands=",",converters={"债券账号": str, "债券代码": str}, header=0)
    df02 = pd.concat([TEMP_df2_1, TEMP_df2_2])
    return df02
'''
第三张表：子公司上清持仓表
'''
def my_sheet3(PATA_DATA,DATA):
    df03 = pd.read_excel(PATA_DATA+"子公司产品持仓债券余额查询"+DATA+".xlsx",thousands=",",converters={"持有人账号": str, "债券代码": str}, header=0)
    return df03

"""第四张表：当日正回购到期"""
def my_sheet4(PATA_DATA,sheet_name,REPO_DATA):
    TEMP_df4 = pd.read_excel(PATA_DATA+sheet_name+".xlsx",thousands=",",converters={"代码": str, "本方托管账号": str}, header=0,index_col=0)
    df04 = TEMP_df4[TEMP_df4["到期结算日"].isin([REPO_DATA])]
    return df04
"""第五张表：质押券管理表"""
def my_sheet5(PATA_DATA):
    df05 = pd.read_excel(PATH_DATA+"质押券管理.xlsx",thousands=",", dtype={"债券代码": str})
    df05["委托代码"] = df05["委托代码"].fillna(method="ffill")
    df05["证券托管户账号"] = df05["证券托管户账号"].fillna(method="ffill")
    return df05
"""第六张表：委托表"""
def my_sheet6(PATA_DATA):
    df06 = pd.read_excel(PATH_DATA+"投资委托查询.xlsx",thousands=",",index_col=0,converters={"债券代码":str})
    return df06
"""第七张表：数据处理表格"""
def my_sheet7(df1,df2,df3,df4):
    bond= df2["债券代码"].tolist()
    bonds = [i + ".IB" for i in bond ]
    mybonds_list=[]
    for i in bonds:
        temp_df = w.wsd(i,"sec_name,net_cnbd,maturitydate,windl2type,amount,latestissurercreditrating","ED-0D", REPO_DATA, "credibility=1;TradingCalendar=NIB;Fill=Previous;PriceAdj=CP",usedf=True)[1]
        mybonds_list.append(temp_df)
    df_bond_info = pd.concat(mybonds_list)
    df_bond_info.index = range(len(df_bond_info))
    df7_1=pd.concat([df2,df_bond_info],axis=1)
    """计算上清可用券"""
    df1["证券托管户账号"] = ["B" + i for i in df1["证券托管户账号"]]
    df1_1 = df1[df1["科目名称"].isin(["可用"])]
    df1_2 = df1_1.groupby(["证券托管户账号","债券代码"])["余额（元）"].sum().reset_index()
    df7_2=df7_1.merge(df1_2, on=["证券托管户账号", "债券代码"], how="left")
    """计算中债可用券"""
    df3_1 = df3.rename(columns={"债券账号":"证券托管户账号"})
    df3_2 = df3_1[df3_1["二级科目名称"].isin(["可用"])]
    df3_3 = df3_2.groupby(["证券托管户账号", "债券代码"])["科目余额（万元）"].sum().reset_index()
    df7_3 = df7_2.merge(df3_3, on=["证券托管户账号", "债券代码"], how="left")
    df7_3["科目余额（万元）"]*=10000
    """计算当日回购到期债券"""
    df4_1 = df4.rename(columns={"本方托管账号":"证券托管户账号","代码":"债券代码"})
    df4_2 = df4_1.groupby(["证券托管户账号", "债券代码"])["券面总额(万)"].sum().reset_index()
    df7_4 = df7_3.merge(df4_2, on=["证券托管户账号", "债券代码"], how="left")
    """计算当日单债委托汇总数"""
    #df7_4["质押总额"] = df7_4["质押总额"].astype("float64")
    df7_5 = df7_4.groupby(["证券托管户账号","债券代码"])["质押总额"].sum().reset_index()
    df7_6 = df7_5.rename(columns={"质押总额":"总质押金额"})
    df7_7 = df7_4.merge(df7_6, on=["证券托管户账号", "债券代码"], how="left")
    df7=df7_7.fillna(0)
    df7["可用券是否足额"]=df7["科目余额（万元）"]+df7["余额（元）"]-df7["质押总额"]
    df7["可用及到期是否足额"]=df7["科目余额（万元）"]+df7["余额（元）"]-df7["质押总额"]+df7["券面总额(万)"]
    df7["总质押量是否超标"]=df7["科目余额（万元）"]+df7["余额（元）"]-df7["总质押金额"]+df7["券面总额(万)"]
    return df7
"""超限预警"""
#def alarm(df7)
#    for i in range (len(df7[债券代码]))：
#        df7.iloc[]




"""
TEMP_df1 = pd.read_excel("C:\\Users\\admin\\Desktop\\stu\\zhg\\test\\上清1217历史持仓余额1.xlsx",converters={"持有人账号":str,"债券代码":str},index_col=0,header=0,)
TEMP_df2 = pd.read_excel("C:\\Users\\admin\\Desktop\\stu\\zhg\\test\\上清1217历史持仓余额2.xlsx",converters={"持有人账号":str,"债券代码":str},index_col=0,header=0,)
TEMP_df3 = pd.read_excel("C:\\Users\\admin\\Desktop\\stu\\zhg\\test\\1217迁移产品上清历史持仓余额.xlsx",converters={"持有人账号":str,"债券代码":str},index_col=0,header=0,)
df01 = pd.concat([TEMP_df1,TEMP_df2,TEMP_df3])

TEMP_df4 = pd.read_excel("C:\\Users\\admin\\Desktop\\stu\\zhg\\test\\1217迁移产品中债总对账单.xlsx",converters={"债券账号":str,"债券代码":str},header=0,)
TEMP_df5 = pd.read_excel("C:\\Users\\admin\\Desktop\\stu\\zhg\\test\\中债1217总对账单.xlsx",converters={"债券账号":str,"债券代码":str},header=0,)
df02 = pd.concat([TEMP_df4,TEMP_df5])

df03 = pd.read_excel("C:\\Users\\admin\\Desktop\\stu\\zhg\\test\\子公司产品持仓债券余额查询1217.xlsx",converters={"持有人账号":str,"债券代码":str},header=0,)
"""
def write_my_excel(df_sheet1, df_sheet2, df_sheet3,df_sheet4,df_sheet5,df_sheet6,df_sheet7):
    with pd.ExcelWriter("a.xlsx") as writer:
        df_sheet1.to_excel(writer,sheet_name="母行及迁移产品上清持仓")
        df_sheet2.to_excel(writer, sheet_name="中债持仓")
        df_sheet3.to_excel(writer, sheet_name="子公司上清持仓")
        df_sheet4.to_excel(writer, sheet_name="正回购到期数据")
        df_sheet5.to_excel(writer, sheet_name="质押券管理")
        df_sheet6.to_excel(writer, sheet_name="投资委托查询")
        df_sheet7.to_excel(writer, sheet_name="数据处理")

if __name__ == '__main__':
    df_mima = pd.read_excel("C:\\Users\\lenovo\\Desktop\\stu\\1224\\mima.xlsx",header=0,index_col=0)
    PATH_DATA = df_mima.iloc[0][0]
    DATA = str(df_mima.iloc[1][0])
    REPO_DATA = df_mima.iloc[2][0]
    sheet_name = df_mima.iloc[3][0]
    #print(PATH_DATA,DATA,sheet_name,REPO_DATA)
    #PATH_DATA = input("请输入文件夹地址：")
    #DATA = input("请输入持仓文件日期（格式例如1224）：")
    #REPO_DATA = input("请输入持仓文件日期（格式例如2021/12/24）：")
    #sheet_name = input("请输入正回购文件名：")
    df_sheet1 = my_sheet1(PATH_DATA,DATA)
    print('===================================母行上清持仓表处理完成=================================')
    df_sheet2 = my_sheet2(PATH_DATA,DATA)
    print('====================================中债持仓表处理完成===================================')
    df_sheet3 = my_sheet3(PATH_DATA,DATA)
    print('=================================子公司上清持仓表处理完成=================================')
    df_sheet4 = my_sheet4(PATH_DATA,sheet_name,REPO_DATA)
    print('===================================正回购到期表处理完成==================================')
    df_sheet5 = my_sheet5(PATH_DATA)
    print('===================================质押券管理表处理完成==================================')
    df_sheet6 = my_sheet6(PATH_DATA)
    print('====================================投资委托表处理完成==================================')
    df_sheet7 = my_sheet7(df_sheet1,df_sheet5,df_sheet2,df_sheet4)
    print('====================================数据处理表处理完成==================================')
    write_my_excel(df_sheet1, df_sheet2, df_sheet3,df_sheet4,df_sheet5,df_sheet6,df_sheet7)
    print('======================================程序运行完毕======================================')


#"C:\Users\lenovo\Desktop\stu\1224\"