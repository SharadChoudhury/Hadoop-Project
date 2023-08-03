# What are the different payment types used by customers and their count? The final results should be in a sorted format.

from mrjob.job import MRJob
from mrjob.step import MRStep


class mapreduce(MRJob):
    def steps(self):
        return [ MRStep(mapper= self.mapper, reducer = self.reducer), 
                MRStep(reducer = self.sort_results)]

    def mapper(self, key, line):
        vals = line.strip().split(',')
        if vals[0] != 'VendorID':
            paymenttype = int(vals[9])
            yield paymenttype,1


    def reducer(self, type, counts):
        yield None, (type, sum(counts))


    def sort_results(self , key, values):
        yield "Payment type", "Counts"
        for type,cnt in sorted(list(values)):
            yield type,cnt


if __name__ == "__main__":
    mapreduce.run()