# How does revenue vary over time? Calculate the average trip revenue per month - analysing it 
# by hour of the day (day vs night) and the day of the week (weekday vs weekend).

from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime


class mapreduce(MRJob):

    def mapper(self, key, line):
        vals = line.strip().split(',')
        if vals[0] != 'id':
            rev = float(vals[11])
            fmt = "%Y-%m-%d %H:%M:%S"
            triptime = datetime.strptime(vals[2], fmt)
            month = triptime.month
            dayofweek = triptime.weekday()  # returns day of week Monday=0, Sunday=6
            hour = triptime.hour
            yield (month,dayofweek,hour),(rev,1)
            

    def reducer(self, times, values):
        trips= 0
        rev = 0
        for r,t in values:
            trips += t
            rev += r
        yield times, round((rev/trips),2)


if __name__ == "__main__":
    mapreduce.run()