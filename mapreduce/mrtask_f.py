# How does revenue vary over time? Calculate the average trip revenue per month - analysing it 
# by hour of the day (day vs night) and the day of the week (weekday vs weekend).

from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime


class mapreduce(MRJob):

    def mapper(self, key, line):
        # this mapper returns the time as (Month,dayofweek,hour) and the fare amount and count for each trip
        # this will give an insight about how many trips and how much revenue is generated from trips by Month,Weekday,Hour
        vals = line.strip().split(',')
        if vals[0] != 'VendorID':
            rev = float(vals[10])
            fmt = "%Y-%m-%d %H:%M:%S"
            # analysing the trip revenue using the pickup time
            triptime = datetime.strptime(vals[1], fmt)
            # extracting month, day of week and hour from the pickup time after converting it to datetime format
            month = triptime.month
            dayofweek = triptime.weekday()  # returns day of week Monday=0, Sunday=6
            hour = triptime.hour
            yield (month,dayofweek,hour),(rev,1)
            

    def reducer(self, times, values):
        # this reducer returns the time in Month,Weekday,Hour and the Avg trip revenue as per the time.
        trips= 0
        rev = 0
        for r,t in values:
            trips += t
            rev += r
        yield times, round((rev/trips),2)


if __name__ == "__main__":
    mapreduce.run()