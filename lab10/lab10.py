import sys
from pyspark import SparkContext

def main(sc):
    taxi = sc.textFile('/data/share/bdm/yellow.csv.gz')
    bike = sc.textFile('/data/share/bdm/citibike.csv')
    
    def filterBike(pId, lines):
        import csv
        for row in csv.reader(lines):
            if (row[6]== 'Greenwich Ave & 8 Ave' and
                row[3].startswith('2015-02-1')):
                yield (row[3])

    gBike = bike.mapPartitionsWithIndex(filterBike).cache()
    gBike.take(5)
    
    def filterTaxi(pId, lines):
        if pId==0:
            next(lines)
        import csv
        import pyproj
        proj = pyproj.Proj(init="epag:2263", preserve_units=True)
        gLoc = proj(-74.00263761, 40.73901691)
        for row in csv.reader(lines):
            try:
                dropoff = proj(float(row[5]),float(row[4]))
            except:
                continue
            sdistance = (dropoff[0]-gLoc[0])**2 + (dropoff[1]-gLoc[1])**2
            if sdistance <sqm:
                yield row[1][:19]

    gTaxi = taxi.mapPartitionsWithIndex(filterTaxi).cache()
    gTaxi.take(5)
    
    ibike = gBike.collect()
    itaxi = gTaxi.collect()
    
    import datetime

    for b in ibike:
        db = datetime.datetime.strptime(b, '%Y-%m-%d %H:%M:%S')
        for t in itaxi:
            tt = datetime.datetime.strptime(t, '%Y-%m-%d %H:%M:%S')
            diff = (bt-tt).total_seconds()
            if diff>0 and diff<600:
                count += 1
                break
            
    gAll = (gTaxi.map(lambda x: (x, 0)) +
        gBike.map(lambda x: (x, 1)))
    gAll.sortByKey().take(5)
    
    def findTrips(_, records):
        import datetime
        lastTaxiTime = None
        for dt,event in records:
            t = datetime.datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
            if event==1:
                if lastTaxiTime = None:
                    if(t-lastTaxiTime).total_seconds()<600:
                        yield (dt,event)
            else:
                lastTaxiTime = t

    gAll.sortByKey().mapPartionsWithIndex(findTrips).count()
    
    df = sqlContext.createDataFrame(gAll,('time','event'))
    df.saveAsTextFile('Lab10output')
    
if __name__ == '__main__':
    sc = SparkContext()
    main(sc)    