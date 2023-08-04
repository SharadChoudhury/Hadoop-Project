# What is the average trip time for different pickup locations?

from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class mapreduce(MRJob):

    def mapper(self, key, value):
        # this mapper pickup location, triptime and count for each trip
        vals = value.strip().split(',')
        if vals[0] != 'VendorID':
            puloc = vals[7]
            fmt = "%Y-%m-%d %H:%M:%S"
            # converting pickup and drop times to datetime format
            picktime = datetime.strptime(vals[1], fmt)
            droptime = datetime.strptime(vals[2], fmt)
            # finding the trip minutes
            tripmins = int((droptime - picktime).total_seconds() // 60)
            yield puloc,(tripmins, 1)


    def reducer(self, puloc, values):
        # the reducer will yield the pickup location and the average trip time
        totaltime = 0
        totalcnt = 0
        for triptime,cnt in values:
            totaltime += triptime
            totalcnt += cnt
        yield puloc , round(totaltime/totalcnt,2)


if __name__ == "__main__":
    mapreduce.run()