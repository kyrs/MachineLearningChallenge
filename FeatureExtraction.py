'''
	library name : score.py
	created by : Kumar Shubham
	Date 10-03-2016

	Description: Following file extract features from the given dataFrame

	Features From Transaction Data :

	1. F_sell : This  is the ratio of net discount given by the merchant to the actuall mrp
	formula = (mrp-(s.p-discount*sp))/mrp

	2. F_product : total no of product selled by a given merchant in given T4 category.

	3. F_dispatch: 
	feature to identify how fast merchant can dispatch the product
	ratio =   (Deadline to deliver the product -product Dispatched by merchant)/( deadline to deliver the product - time when merchant was informed)

	case 1: 
	dispatch and info is provided before deadline
	F_dispatch = ratio

	case 2: 
	dispatch and info both are provided after deadline
	F_dispatch = 1/ratio

	case 3:
	info is provided 6hr before deadline and inspite of that  merchant failed to deliver
	F_dispatch = ratio

	case 4 : info is provided after 6hr and still he fail to deliver (delay in providing info to merchant)
	F_dispatch = 1/ratio

	Assumption: merchant need 6hr prior info for proper handling of the good 

	Feature for cancel Data:

	F_csatisfy = ratio of product not returned by customer to that of total product ordered(collected from transaction data)
	F_psent  = ratio of product sent by merchant to that of total product ordered(collected from transaction data)

	Feature for profit Data:
	F_profit: comission given by merchant to paytm

'''



import pandas as pd
import numpy as np 
import json 
from datetime import datetime
import time
from merchantdatetime import diff

class featureExtraction(object):
	
	def __init__(self,transdata,returnData,profitData):
		self.transData = transdata
		self.returnData = returnData
		self.profitData = profitData

	def transFeatureExtraction(self):
		## function for extracting the transaction Feature
		shippingRatio = self.__shippingDetail()

		sellDetail = self.__sellDetail()

		col = ["T1","T2","item_mrp","item_selling_price","item_discount","fulfillment_created_at","item_ship_by_date","fulfillment_shipped_at"]
		self.transData.drop(col,inplace = True,axis =1)
		newDataFrame = pd.concat([self.transData,sellDetail,shippingRatio],axis=1)
		newDataFrame.loc[newDataFrame["F_sell"]>1,'F_sell'] = np.median(newDataFrame["F_sell"])
		Data = self.__transFeature(newDataFrame)
		return Data


	def __shippingDetail(self):
		## function calculating the shipping features from the transaction dataFrame
		print "Extracting Shipping Details !!"
		df = self.transData.apply(self.__dateExtraction,axis=1)
		df = pd.DataFrame(df,columns=['F_dispatch'],dtype = "float")
		#print df
		return df


	def __dateExtraction(self,x):
		##function called in apply function for calculation of shipping features from each row
		merchantAsked = "fulfillment_created_at"
		deadlineDelivery = "item_ship_by_date"
		itemShippedAt = "fulfillment_shipped_at"
		dateFormat  = "%Y-%m-%d %H:%M:%S"

		dateMerchant = x[merchantAsked].replace(".0","")
		dateDeadline = x[deadlineDelivery].replace(".0","")
		dateItemShippedAt = x[itemShippedAt].replace(".0","")

		if (dateMerchant != "null" and dateDeadline != "null" and dateItemShippedAt != "null"):
			
			dateMerchant = datetime.strptime(dateMerchant,dateFormat)
			dateDeadline = datetime.strptime(dateDeadline,dateFormat)
			dateItemShippedAt =datetime.strptime(dateItemShippedAt,dateFormat)

			try:
				shipment = float(diff(dateDeadline,dateItemShippedAt))
				information = float(diff(dateDeadline,dateMerchant))

				if (shipment>0 and information>0):
					# condition where shipment is done before deadline and information is given before deadline
					if (information > shipment):
						ratio = shipment/information
					else:
						ratio = 0
				elif (shipment<0 and information <0):
					# shipment and information is provided after deadline
					if abs(shipment) > abs(information):
						ratio = information/shipment
					else:
						ratio = 0

				elif( information > 21600 and shipment <0):
					# information regarding merchant is provided 6hr before deadline and merchant failed to ship it in deadline
					ratio = shipment/information
				else:
					# information is provided less than 6hr to deadline
					ratio = information/shipment


				
			except ZeroDivisionError :
				ratio = 0
		else:
			ratio =0

		return ratio

	def __sellDetail(self):
		mrp = "item_mrp"
		sp = "item_selling_price"
		discount = "item_discount"
		print "Extracting Sell Features !!"
		df = (self.transData[mrp] - (self.transData[sp] - self.transData[sp]*self.transData[discount]))/self.transData[mrp]
		df = pd.DataFrame(df,columns=["F_sell"],dtype = "object")
		return df

	def __transFeature(self,dataGenerated):
		## Function creating a general summary of a given merchant for a given T4 
			# 1. Avg Selling ratio
			# 2. Avg selling dispatch ratio
			# 3. total no of product in a given category selled by a given merchant
			## this file extract meaningful features from big size csv file and compress it to small size 
		print "Starting transFeature extraction !!"
		unq_merchant = list(set(dataGenerated["merchant_id"]))
		listData =[]
		for merchant in unq_merchant:
			print unq_merchant.index(merchant)
			
			data_merchant = dataGenerated[self.transData["merchant_id"] == merchant]
			t4List = list(set(data_merchant["T4"]))
			#print len(t4List)
			for t4 in t4List:
				dictData = {}
				dataT4 = data_merchant[data_merchant["T4"]== t4]
				dictData["merchant_id"] = merchant 
				dictData["T4"] = t4
				dictData["F_product"] = np.sum(dataT4["qty_ordered"])
				dictData["F_avgsell"] = np.mean(dataT4["F_sell"])
				dictData["F_avgdispatch"] = np.mean(dataT4["F_dispatch"])
				listData.append(dictData)
		df = pd.DataFrame(listData)
		return df 


	def cancelFeatureExtraction(self,dataFrame):
		## Function calculate the cancelFeature
		# F_psent : ratio of product shipped by merchant to total product ordered from the merchant. Total product idea basically come from transFeatures calculated earlier for NA
		#			we have considered median + thershold for total product asked

		# F_csatisfy : ratio of product not returned back by customer compared to total product ordered

		transModifiedData = dataFrame
		medianProductSelled = np.median(transModifiedData["F_product"])
		data = self.returnData
		print data.columns
		print transModifiedData.columns
		NewDataFrame = pd.merge(data,transModifiedData, on = ["merchant_id","T4"],how ="left")
		print NewDataFrame.columns
		NewDataFrame.loc[:,"F_product"] = NewDataFrame["F_product"].fillna(medianProductSelled+5)
		
		F_psent = (NewDataFrame["F_product"]-NewDataFrame["cancel_num"])/NewDataFrame["F_product"]
		F_csatisfy = (NewDataFrame["F_product"]-NewDataFrame["return_num"])/NewDataFrame["F_product"]

		F_psent = pd.DataFrame(F_psent,columns=["F_psent"],dtype = "object")
		
		F_csatisfy = pd.DataFrame(F_csatisfy,columns=["F_csatisfy"],dtype = "object")

		
		col = ["T1","T2","F_product","cancel_num","return_num","F_avgsell","F_avgdispatch"]
		NewDataFrame.drop(col,inplace = True,axis =1)
		
		NewDataFrame = pd.concat([NewDataFrame,F_psent,F_csatisfy],axis=1)
		NewDataFrame.loc[NewDataFrame["F_csatisfy"]<0,"F_csatisfy"] = 1
		NewDataFrame.loc[NewDataFrame["F_psent"]<0,"F_psent"] = 1
		print NewDataFrame[1:10]
		return NewDataFrame

	def profitFeatureExtraction(self):
		## extracting the profit feature: This basically contain the commision collected by paytm for given merchant

		profitData = self.profitData
		col = ["T1","T2",'discount_percent', "cashback_percent"]
		profitData.drop(col,inplace=True,axis=1)
		profitData.rename(columns={"commission_percent": 'F_profit'}, inplace=True)
		return profitData





if __name__ == "__main__":
	print "reading the Data"
	# transData = pd.read_csv("./data5000/transData5000.csv")
	
	#cancelData = pd.read_csv("./data5000/cancelData5000.csv") 
	profitData = pd.read_csv("./data5000/profitData5000.csv")
	# #transData = pd.read_csv("./transF1.csv")
	# print "starting Feature Extraction"
	# paytm = featureExtraction(transData,cancelData,profitData)
	paytm = featureExtraction("-","-",profitData)
	# ## freeing the memory
	# del transData
	# del cancelData
	# del profitData
	#temp = paytm.transFeatureExtraction()
	#temp.to_csv("transF1.csv",index=False)
	#temp.to_csv("NewTransFeatureF1.csv",index=False)

	#dataNew = pd.read_csv("NewTransFeatureF1.csv")
	#temp = paytm.FeatureExtraction(dataNew)

	temp = paytm.profitFeatureExtraction()
	temp.to_csv("NewprofitFeatureF1.csv",index= False)