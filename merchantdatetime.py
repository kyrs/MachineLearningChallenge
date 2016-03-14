from datetime import datetime
from dateutils  import relativedelta



def diff (a1,b1):
	## function calculating relative time diff between two timeFrame in sec

	differance = relativedelta(a1,b1)	
	temp = differance.years*365*24*60*60+ differance.months*30*24*60*60+differance.days*24*60*60+differance.hours*60*60+differance.minutes*60+differance.seconds
	

	return temp

if __name__ == "__main__":
	a = datetime(2015,07,8,17,1,42)
	b = datetime(2015,07,07,19,00,57)
	print diff(a,b)