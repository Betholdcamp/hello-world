from pyspark import SparkContext
from collections import Counter
from pyspark.sql import SQLContext

if __name__ == '__main__':
    sc = SparkContext()
    complains = sc.textFile('complaints_sample.csv', use_unicode=True).cache()
    
    def extractinfo(partId, list_of_records):
        if partId==0:
            next(list_of_records) # skipping the first line
        
        import csv
        reader = csv.reader(list_of_records)
        for row in reader:
            (years, product, company) = (row[0], row[1], row[7])
            year = years.split('-')[0]
            yield (product, (year, 1, (company, 1)))
         
    
    company_info = complains.mapPartitionsWithIndex(extractinfo)
    test = company_info.map(lambda x: ((x[0], int(x[1][0])),  (set([x[1][2][0]]), x[1][2][1], {x[1][2][0]: 1}))) \
    .reduceByKey(lambda x,y: (x[0].union(y[0]), x[1]+y[1], Counter(x[2]) + Counter(y[2]))) \
    .mapValues(lambda x: (len(x[0]), x[1], int((max(x[2].values())/x[1]*100)))) \
    .sortByKey()
    
    final = test.map(lambda x: (x[0][0], x[0][1], x[1][1], x[1][0], x[1][2])) 
    
    sqlContext = SQLContext(sc)
    df = sqlContext.createDataFrame(final, ('Product', 'Year', 'Total_Complains', 'Unique_Companies', 'Percentage'))
    
    df.write \
    .format('com.databricks.spark.csv') \
    .save('output.csv')
    
    

   