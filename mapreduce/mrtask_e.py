# Calculate the average tips to revenue ratio (total tips/total revenue) of the drivers for 
# different pickup locations in sorted format.

from mrjob.job import MRJob
from mrjob.step import MRStep


class mapreduce(MRJob):

    def steps(self):
        return [ MRStep(mapper = self.mapper, reducer = self.reducer),
                    MRStep(reducer = self.sorted_ratios) ]

    def mapper(self, key, value):
        vals = value.strip().split(',')
        if vals[0] != 'VendorID':
            puloc = vals[7]
            tip = float(vals[13])
            rev = float(vals[10])
            yield puloc,(tip, rev)


    def reducer(self, puloc, values):
        totaltip = 0
        totalrev = 0
        for tip,rev in values:
            totaltip += tip
            totalrev += rev
        yield None, (totaltip/totalrev, puloc)


    def sorted_ratios(self, key, values):
        values = sorted(values)
        yield "Pickup location", "Tip to revenue ratio"
        for ratio,loc in values:
            yield loc, round(ratio,2)


if __name__ == "__main__":
    mapreduce.run()