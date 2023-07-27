# What is the average trip time for different pickup locations?

from mrjob.job import MRJob
from mrjob.step import MRStep
from datetime import datetime

class mapreduce(MRJob):

    def mapper(self, key, value):
        vals = value.strip().split(',')
        if vals[0] != 'id':
            puloc = vals[8]
            fmt = "%Y-%m-%d %H:%M:%S"
            picktime = datetime.strptime(vals[2], fmt)
            droptime = datetime.strptime(vals[3], fmt)
            tripmins = int((droptime - picktime).total_seconds() // 60)
            yield puloc,(tripmins, 1)


    def reducer(self, puloc, values):
        totaltime = 0
        totalcnt = 0
        for triptime,cnt in values:
            totaltime += triptime
            totalcnt += cnt
        yield puloc , round(totaltime/totalcnt,2)


if __name__ == "__main__":
    mapreduce.run()