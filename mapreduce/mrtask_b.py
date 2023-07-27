# Which pickup location generates the most revenue? 

from mrjob.job import MRJob
from mrjob.step import MRStep


class mapreduce(MRJob):

    def steps(self):
        return [ MRStep(mapper = self.mapper, reducer = self.reducer),
                    MRStep(reducer = self.max_revenue) ]

    def mapper(self, key, value):
        vals = value.strip().split(',')
        if vals[0] != 'id':
            puloc = vals[8]
            rev = float(vals[11])
            yield puloc,rev


    def reducer(self, puloc, rev):
        yield None, (sum(rev), puloc)


    def max_revenue(self, key, values):
        maxval = max(values, key=lambda x : x[0])
        yield "Pickup location with highest revenue", maxval[1]
        yield "Revenue", maxval[0]


if __name__ == "__main__":
    mapreduce.run()