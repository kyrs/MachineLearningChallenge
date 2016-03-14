'''
    Paytm Machine Learning Challenge

    Problem: Rating the Merchant
    Created By: Kumar Shubham
    Date: 10/03/2016
'''

import pandas as pd
import numpy as np 

from FeatureExtraction import featureExtraction
from score import transResultGeneration,cancelResultGeneration,profitResultGeneration
from merge import mergePaytm

class slice(object):
    ## class for slicing the dataFrame and saving it in sliced Folder
    ## Done for big dataFrame for proper computation

    def __init__(self,Data,Folder):
        self.data = Data 
        self.folder = Folder

    def slice(self,name):
        ## method for slicing the dataFrame
        merchant = list(set(self.data["merchant_id"]))
        # splitting merchant frame into 5 equal part
        splittedFrame = self.__split_seq(merchant,5)

        for i in range(len(splittedFrame)):
            FrameName = self.folder+name+str(i+1)+".csv"
            temp = self.data[self.data["merchant_id"].isin(splittedFrame[i])]
            temp.to_csv(FrameName,index=False)

    def __split_seq(self, seq, size):
        ## private function for splitting a list in k size
        newseq = []
        splitsize = 1.0/size*len(seq)
        for i in range(size):
                newseq.append(seq[int(round(i*splitsize)):int(round((i+1)*splitsize))])
        return newseq


if __name__ == "__main__":
    cacheFolder = "./cache/"
    fileSlice = "trans"
    resultFolder = "./result/"
    print "Loading and processing Transaction"
    transData = pd.read_csv("./trans/transaction")
    paytmSlice =slice(transData,cacheFolder)
    paytmSlice.slice(fileSlice)

    ####################################################
    # Extracting the features from all the 5 data frame#
    ####################################################
    print "Reading the slice Frame"
    for i in range(5):
        fileName = fileSlice+str(i+1)
        dataFrame = pd.read_csv(cacheFolder+fileName+".csv")
        paytmFeature =featureExtraction(dataFrame,"-","-")
        temp = paytmFeature.transFeatureExtraction()
        print ("saving "+ filename )
        temp.to_csv(cacheFolder+fileName+"F.csv",index=False)
        del dataFrame
        del temp
        del paytmFeature

    #####################################################
    # Merging all the DataFrame into one DataFrame      #
    #####################################################

    print "Merging The dataFrame"
    listFile = [cacheFolder +fileSlice+str(i+1)+"F.csv" for i in range(5)]
    print listFile
    dataFrameList = []
    for i in listFile:
        df = pd.read_csv(i)
        dataFrameList.append(df)

    finalFrame = pd.concat(dataFrameList)
    finalFrame.to_csv(cacheFolder+"transFinal.csv",index=False)

    del finalFrame

    #####################################################
    # extracting Features of cancel and profit Data     #
    #####################################################

    print "Extracting features of profit and cancel data"

    transFeature = pd.read_csv(cacheFolder+"transFinal.csv")
    cancelData = pd.read_csv("./cancel/cancel")
    profitData = pd.read_csv("./profit/profit")
    paytmFeature = featureExtraction("-",cancelData,profitData)
    print "Extracting cancel Features"
    cancelFeature = paytmFeature.cancelFeatureExtraction(transFeature)
    cancelFeature.to_csv(cacheFolder+"cancelFinal.csv",index = False)

    print "Extracting profitFeatures"
    profitFeature = paytmFeature.profitFeatureExtraction()
    profitFeature.to_csv(cacheFolder+"profitFinal.csv",index = False)

    del transFeature
    del profitData
    del paytmFeature

    ######################################################
    # Giving the final score to all the patient          #
    ######################################################
    transData = pd.read_csv(cacheFolder+ "transFinal.csv")
    profitData = pd.read_csv(cacheFolder+"profitFinal.csv")
    cancelData = pd.read_csv(cacheFolder+"cancelFinal.csv")

    print "Trans Score Calculation"
    temp = transResultGeneration(transData)
    print temp
    temp.to_csv(resultFolder+"transScore.csv",index=False)

    print "Profit Score calculation"
    temp = profitResultGeneration(profitData)
    temp.to_csv(resultFolder+"profitScore.csv",index=False)

    print "cancel Score calculation"
    temp = cancelResultGeneration(cancelData)
    temp.to_csv(resultFolder+"cancelScore.csv",index=False)

    del temp

    transData = pd.read_csv("./result/transScore.csv")
    profitData = pd.read_csv("./result/profitScore.csv")
    cancelData = pd.read_csv("./result/cancelScore.csv")
    temp = mergePaytm(transData,profitData,cancelData)
    temp.to_csv("./result/finalResult.csv",index = False)

