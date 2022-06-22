from pyspark import SparkContext
from pyspark.streaming import StreamingContext


def updateFunc(newValues,previousState):
    if previousState is None:
        previousState=0
    return sum(newValues,previousState)

if __name__ == "__main__":
    sc = SparkContext("local[*]","State_Full_Transformation")
    sc.setLogLevel("ERROR")
    
    ssc = StreamingContext(sc,5)
    ssc.checkpoint("checkpoint") #CheckPoint :- In stateful transformation to remember state we have to use checkPoint directory.
    lines = ssc.socketTextStream("localhost",9091)
    words = lines.flatMap(lambda x:x.split())
    pairs = words.map(lambda x:(x,1))
    words_counts = pairs.updateStateByKey(updateFunc)
    words_counts.pprint()
    ssc.start()

    ssc.awaitTermination()