import sys
from pyspark import SparkContext

def main(sc):
    RES_NYC ='/data/share/bdm/nyc_restaurants.csv'
    Cus = sc.textFile(RES_NYC, use_unicode=False).cache()
    #list(enumerate(sat.first().split(',')))
    
    def extractCuisines(partId, records):
    if partId==0:
        records.next()
    import csv
    reader = csv.reader(records)
    for row in reader:
        (cuisine,count) = (row[7],1)
        yield(cuisine,count)
        
    CusScores = Cus.mapPartitionsWithIndex(extractCuisines) \
                .reduceByKey(lambda accum, n: accum + n) \
                .saveAsTextFile('hw4output')
            
if __name__ == '__main__':
    sc = SparkContext()
    main(sc)