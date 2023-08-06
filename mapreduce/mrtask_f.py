# How does revenue vary over time? Calculate the average trip revenue per month - analysing it 
# by hour of the day (day vs night) and the day of the week (weekday vs weekend).

from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime


class mapreduce(MRJob):

    def mapper(self, key, line):
        # this mapper returns the time as (Month,weekend/weekend,day/night) and the fare amount and count for each trip
        # this will give an insight about how many trips and how much revenue is generated from trips by Month,weekend/weekend,day/night
        vals = line.strip().split(',')
        if vals[0] != 'VendorID':
            rev = float(vals[10])
            fmt = "%Y-%m-%d %H:%M:%S"
            # analysing the trip revenue using the pickup time
            triptime = datetime.strptime(vals[1], fmt)
            # extracting month, day of week and hour from the pickup time after converting it to datetime format
            month = triptime.month
            dayofweek = triptime.weekday()              # returns day of week Monday=0, Sunday=6
            hour = triptime.hour

            weekday = "Weekday"                         # indicates its weekday or weekend 
            day = "Night"                               # indicates its day or night 
            if dayofweek == 5 or dayofweek == 6:        # saturdays and sundays are weekends
                weekday = "Weekend"
            if hour >= 6 and hour <= 18:                # assuming it's day time from 6am to 6pm
                day = "Day"
            yield (month,weekday,day),(rev,1)
            

    def combiner(self, times, values):
        # this combiner returns the time in Month,weekend/weekend,day/night and the total trips and total revenue for each partition.
        trips= 0
        rev = 0
        for r,t in values:
            rev += r
            trips += t           
        yield times,(rev,trips)
    

    def reducer(self, times, values):
        # this reducer returns the time in Month,weekend/weekend,day/night and the Avg trip revenue as per the time.
        trips= 0
        rev = 0
        for r,t in values:
            rev += r
            trips += t
        yield times, round((rev/trips),2)



if __name__ == "__main__":
    mapreduce.run()