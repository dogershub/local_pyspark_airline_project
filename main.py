#pip install pyspark

import pyspark
#imporint SparkSession
from pyspark.sql import SparkSession

#creating a sparksession object and providing appName
spark=SparkSession.builder.appName("business_case").getOrCreate()

#----> if you already have a SparkSession running you have to stop them
#spark.stop()

#To create dataframe form External datasets
#does it have a header column
#load the CSV
AirlineDF = spark.read.option("header","true").csv("./Data/airlines1.csv")

#show first 20
# AirlineDF.show()

#create multiple dataframes as per data information
#partitioning for certain needs

AirlineTimeInfo = AirlineDF.select("_c0","Year","Quarter", "Month", "DayofMonth", "DayofWeek" , "FlightDate" )
AirlineInfo = AirlineDF.select("_c0","Reporting_Airline","DOT_ID_Reporting_Airline", "IATA_CODE_Reporting_Airline", "Tail_Number", "Flight_Number_Reporting_Airline" )
AirlineOriginInfo = AirlineDF.select("_c0","OriginAirportID","OriginAirportSeqID", "OriginCityMarketID", "Origin", "OriginCityName" , "OriginState", "OriginStateFips", "OriginStateName", "OriginWac" )
AirlineDestinationInfo = AirlineDF.select("_c0","DestAirportID","DestAirportSeqID", "DestCityMarketID", "Dest", "DestCityName" , "DestState", "DestStateFips", "DestStateName", "DestWac" )
AirlineDepartureInfo = AirlineDF.select("_c0","CRSDepTime","DepTime", "DepDelay", "DepDelayMinutes", "DepDel15" , "DepartureDelayGroups", "DepTimeBlk", "TaxiOut", "WheelsOff" )
AirlineArrivalInfo = AirlineDF.select("_c0","WheelsOn","TaxiIn", "CRSArrTime", "ArrTime", "ArrDelay" , "ArrDelayMinutes", "ArrDel15", "ArrivalDelayGroups", "ArrTimeBlk" )
AirlineCanDivInfo = AirlineDF.select("_c0","Cancelled","CancellationCode", "Diverted" )
AirlineSummry = AirlineDF.select("_c0","CRSElapsedTime","ActualElapsedTime", "AirTime", "Flights", "Distance" , "DistanceGroup" )
AirlineDelayInfo = AirlineDF.select("_c0","CarrierDelay","WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay" ) 

#AirlineTimeInfo.summary().show()
# AirlineTimeInfo.show()



# calculate total no of flights
AirlineTimeInfo.select('_c0').count()

# #SQL
# AirlineTimeInfo.createOrReplaceTempView("mytable")
# sql_count = spark.sql("SELECT count(_c0) FROM mytable")
# sql_count.show()

# select year form 2000 to 2020
# AirlineTimeInfo.select('Year','FlightDate').filter((AirlineTimeInfo.Year >= 2015) & (AirlineTimeInfo.Year <= 2020)).show()

# #SQL
AirlineTimeInfo.createOrReplaceTempView("mytable")
sql_count = spark.sql("SELECT Year,FlightDate FROM mytable WHERE (Year >= 2015) AND (Year <= 2020) ORDER BY Year")
sql_count.show()