###########################################
#	Paytm Machine Learning Challenge  #
###########################################

Date Given : 25 Feb 2016
Date started: 3 march 2016
created by Kumar shubham
email id: kumarshubham652@gmail.com
-------------------------------------------------------
library used: 
	1.pandas
	2. multiprocessing
	3. numpy 
	4. dateutils
	5. datetime
	6. multiprocessing

platform: python

hardware used: Ubuntu 8GB ram + 8 core processor 

How to run :
-----------------------------------------------
	1.	put transaction data in trans Folder renamed as transaction
	2. 	put the profit data in profit Folder renamed as profit
	3. 	put the cancel Data in cancel data folder renamed as cancel

	4. 	run main.py


Key Feature:
--------------------------------------------------
	since Python give an excellent input output buffer system and with present memory restriction. Data has been divided into multiple 		frame and processed acoordingly

	use of Bradely-Terry(sigmoid adaptation) for rating

	
Description of file:

	FeatureExtraction file :
--------------------------------------------------
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

 -------------------------------------------
Scoring:

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




