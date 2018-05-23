import sys
from pyspark import SparkContext

def extractAgency(partId, records):
    if partId==0:
        records.next()
    import csv
    reader = csv.reader(records)
    for row in reader:
        (agencies,count) = (row[3],1)
        yield(agencies,count)

def main(sc):
    NYC311 = "/user/ywu004/311.csv"
    Serv = sc.textFile(NYC311, use_unicode=False)
    #list(enumerate(sat.first().split(',')))
    Serv.take(2)
    
    StatScores = Serv.mapPartitionsWithIndex(extractAgency) \
                     .reduceByKey(lambda accum, n: accum + n)
    SS = StatScores.collect()
    SS.saveAsTextFile('311AgenciesOutput')       
            
if __name__ == '__main__':
    sc = SparkContext()
    main(sc)