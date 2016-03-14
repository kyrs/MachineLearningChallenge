'''
	File for merging the csv files into one file 
	created by KumarShubham
'''
import pandas as pd
import numpy as np 

def mergePaytm(transResult,profitResult,cancelResult):
	newFrame1 = pd.merge(transResult,profitResult,on=["merchant_id","T4"],how = "outer")
	newFrame2 = pd.merge(transResult,cancelResult,on= ["merchant_id","T4"],how = "outer")
	finalFrame = pd.merge(newFrame2,newFrame1,on=["merchant_id","T4"],how = "left")
	finalFrame.rename(columns={"transScore_x": 'transScore'}, inplace=True)
	finalFrame.drop("transScore_y",inplace = True,axis =1)
	# print finalFrame.columns
	finalFrame.loc[:,"transScore"] = finalFrame["transScore"].fillna(0.5)
	finalFrame.loc[:,"profitScore"] = finalFrame["profitScore"].fillna(0.5)
	finalFrame.loc[:,"cancelScore"] = finalFrame["cancelScore"].fillna(1)

	temp = 0.4*finalFrame["transScore"] + 0.4*finalFrame["cancelScore"] + 0.2*finalFrame["profitScore"]
	finalFrame["FinalScore"] = pd.Series(temp , index = finalFrame.index)
	return finalFrame


if __name__ =="__main__":

	transData = pd.read_csv("./result/data/transScore.csv")
	profitData = pd.read_csv("./result/data/profitScore.csv")
	cancelData = pd.read_csv("./result/data/cancelScore.csv")
	temp = mergePaytm(transData,profitData,cancelData)
	temp.to_csv("./result/data/finalResult.csv",index = False)
