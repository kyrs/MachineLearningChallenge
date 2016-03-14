'''
library name : score.py
created by : Kumar Shubham
Date 10-03-2016
Description: Following file calculate the score for three data set seperately

idea : Rating of merchant is done on a competative manner. Each merchant is compared with merchant in same T4 category and rated on the basis of their performance
	   EACH T4 CATEGORY IS TREATED SEPERATELY AS MERCHANT DATA IS HIGHLY UNCOORELATED 

e.g. It might happen that shippment of washing machine might take longer than that of a phone
or washing machine might be ordered in low quantity. compared to phone from a merchant.
Now, Obviously we can't compare merchant selling washing machine and mobile as they might be performing good in their respective
category but not as a whole.

Transaction Data score:
rating of a merchant for a given T4 category is done using Bradely-Terry(Sigmoid adaptation) 

Profit Data score :
concept of Transaction data score is applied on Feature of profit data

Cancel Data Score: 
As told earlier, two feature has been calculated from the cancel Data
ratio1 : ratio of product customer accept from total product
ratio2 : ratio of shipped product from total product

final score is weight sum of the ratio. 0.6 *ratio1 +0.4*ratio 2
with the  assumption that for paytm delivery of good product matter more than cancellation of product by merchant




'''

import pandas as pd 
import numpy as np 
from multiprocessing import Pool
import itertools


def scaleTransData(dataFrame):
	## scale the data only if there is any other data for comparision
	column = "F_product" 
	if len(dataFrame)>1:
		dataFrame.loc[:,column] = (dataFrame[column]-dataFrame[column].min())/ dataFrame[column].max()
	return dataFrame

def ABetterB(A,B):
	## computing the Bradely-Terry model(Sigmoid adaptation) to compare two merchant

	fec = np.sum(np.array(A)-np.array(B))
	res  = 1/(1+np.exp(-fec))
	return res

def computeScore(dataFrame,T4,colScore):
	## function comparing score of two mercant for given T4 using Bradely-Terry model(Sigmoid adaptation) 	
	merchant  = dataFrame['merchant_id']
	T4Data = []
	for merchant_A  in list(merchant):
		meanScoreList = []
		dictScoreList = {}
		merchantA = dataFrame.ix[dataFrame["merchant_id"]==merchant_A,colScore]
		competition = list(set(merchant) - set([merchant_A]))
		if len(competition)<1:
			## if there is no competitor for given T4 then Bradely score is 0.5 no need to compare
			meanScoreList.append(0.5)
		else:
			for merchant_B in competition:
				merchantB = dataFrame.ix[dataFrame["merchant_id"]==merchant_B,colScore]
				temp =ABetterB(merchantA,merchantB)
				meanScoreList.append(temp)
		dictScoreList["merchant_id"] = merchant_A
		dictScoreList["score"] = np.mean(meanScoreList)
		dictScoreList["T4"] = T4
		T4Data.append(dictScoreList)

	return T4Data


def computeScore2(dataFrame):
	## function comparing score of two mercant for given T4 using Bradely-Terry model(Sigmoid adaptation) 
	if "F_profit" in list(dataFrame.columns):
		colScore = ["F_profit"]
	else:
		colScore= ["F_product","F_avgsell","F_avgdispatch"]
	
	merchant  = dataFrame['merchant_id']
	T4 = list(dataFrame["T4"])[0]
	T4Data = []
	for merchant_A  in list(merchant):
		meanScoreList = []
		dictScoreList = {}
		merchantA = dataFrame.ix[dataFrame["merchant_id"]==merchant_A,colScore]
		competition = list(set(merchant) - set([merchant_A]))
		if len(competition)<1:
			## if there is no competitor for given T4 then Bradely score is 0.5 no need to compare
			meanScoreList.append(0.5)
		else:
			for merchant_B in competition:
				merchantB = dataFrame.ix[dataFrame["merchant_id"]==merchant_B,colScore]
				temp =ABetterB(merchantA,merchantB)
				meanScoreList.append(temp)
		dictScoreList["merchant_id"] = merchant_A
		dictScoreList["score"] = np.mean(meanScoreList)
		dictScoreList["T4"] = T4
		T4Data.append(dictScoreList)

	return T4Data


def transResultGeneration(dataFrame):
	# Function for genrating the trans score
	print "TransResultGenerating !!"	
	col = ["F_product","F_avgsell","F_avgdispatch"]
	temp = ResultGeneration2(dataFrame,col,trans = True)
	temp.rename(columns={"score": 'transScore'}, inplace=True)
	return temp

def ResultGeneration(data,colscore,trans = False):
	## using simple "for loop" for calculation of score for each T4 category
	unqT4 = list(set(data["T4"]))
	outputList = []
	for T4 in unqT4:
		print unqT4.index(T4)
		dataFrame = data[data["T4"] == T4 ]
		print trans
		if (trans):
			dataFrame = scaleTransData(dataFrame)
		outputList.extend(computeScore(dataFrame,T4,colscore))

	result = pd.DataFrame(outputList)
	return result
	#result.to_csv("final_result.csv",index=False)

def ResultGeneration2(data,colscore, trans =False):
	## Applying multiPorocessing for calculation of the score for each T4 category
	unqT4 = list(set(data["T4"]))
	outputList = []
	DataFrameList = []
	print "using parallel processing"
	for T4 in unqT4:
		print unqT4.index(T4)
		dataFrame = data[data["T4"] == T4 ]
		if (trans):
			dataFrame = scaleTransData(dataFrame)
		
		DataFrameList.append(dataFrame)
	

	result = Pool(processes=7).map( computeScore2,DataFrameList)
	
	result = list(itertools.chain(*result)) 

	result = pd.DataFrame(result)
	return result




def cancelResultGeneration(dataFrame):
	# Function for genrating the cancel score
	temp = (0.6*dataFrame["F_csatisfy"] + 0.4*dataFrame["F_psent"])
	dataFrame["cancelScore"] = pd.Series(temp , index = dataFrame.index)
	col = ["F_psent","F_csatisfy"]
	dataFrame.drop(col,inplace = True,axis =1)
	return  dataFrame

def profitResultGeneration(dataFrame):
	## function for genereating the Profit score 
	colscore = "F_profit"
	temp = ResultGeneration2(dataFrame,colscore,trans = False)
	temp.rename(columns={"score": 'profitScore'}, inplace=True)
	return temp

if __name__ == "__main__":
	print "Reading The Data"
	#data = pd.read_csv("NewTransFeatureF1.csv")
	#colscore = ["F_product","F_avgsell","F_avgdispatch"]
	#ResultGeneration(data,colscore,scale=True)
	
	#data = pd.read_csv("NewCancelFeatureF1.csv")
	#dataFrame = cancelResultGeneration(data)

	data = pd.read_csv("NewprofitFeatureF1.csv")
	dataFrame = profitResultGeneration(data)