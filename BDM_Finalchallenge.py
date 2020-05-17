import sys
from pyspark import SparkContext
from collections import Counter
import re

if __name__ == '__main__':
    sc = SparkContext()
    streets = sc.textFile('/data/share/bdm/nyc_cscl.csv', use_unicode=False).cache() #'/data/share/bdm/complaints.csv'
    all_tickets = sc.textFile('/data/share/bdm/nyc_parking_violations/', use_unicode=False).cache()
    
    def lines(partId, records):
        if partId==0:
            next(records) 
        
        import csv
        reader = csv.reader(records)
        for row in reader:
            if row[0] != '' and row[0] != '' and row[4] != '' and row[5] != '':
                (PHYSICALID, L_LOW_HN, L_HIGH_HN, R_LOW_HN, R_HIGH_HN, ST_LABEL, BOROCODE, FULL_STREE) = (row[3], row[0], row[1], row[4], row[5], row[10], row[13], row[28])
                L_low1 = re.findall(r'\d+',L_LOW_HN)
                L_low = list(map(int, L_low1))
                
                L_high1 = re.findall(r'\d+',L_HIGH_HN)
                L_high = list(map(int, L_high1))
                
                R_low1 = re.findall(r'\d+',R_LOW_HN)
                R_low = list(map(int, R_low1))
                
                R_high1 = re.findall(r'\d+',R_HIGH_HN)
                R_high = list(map(int, R_high1))
                
                borocode = ('Unknown', 'NY' , 'BX', 'K', 'Q', 'R')
                yield (int(PHYSICALID), (L_low, L_high), (R_low, R_high), ST_LABEL, borocode[int(BOROCODE)], FULL_STREE)
    
    street_line = streets.mapPartitionsWithIndex(lines)
    street_list = street_line.collect()
    
    def findid(borough, street, h_num):
        dd = None
        for i in street_list:
            if (i[3] == street or i[5] == street) and i[4]==borough:
                if (len(h_num) == 1) and (h_num[0] % 2 == 0) and (h_num[0] >= i[2][0][0] and h_num[0] <= i[2][1][0]):
                    dd = i[0]
                    break
                elif (len(h_num) == 1) and (h_num[0] % 2 != 0) and (h_num[0] >= i[1][0][0] and h_num[0] <= i[1][1][0]):
                    dd = i[0]
                    break
                elif (len(h_num) == 2) and (h_num[1] % 2 == 0) and (len(i[2][0])==2) and (len(i[2][1])==2) and (h_num[1] >= i[2][0][1] and h_num[1] <= i[2][1][1]):
                    dd = i[0]
                    break  
                elif (len(h_num) == 2) and (h_num[1] % 2 != 0) and (len(i[1][0])==2) and (len(i[1][1])==2) and (h_num[1] >= i[1][0][1] and h_num[1] <= i[1][1][1]):
                    dd = i[0]
                    break 
            else:
                dd = None
                break
        return dd
    
    def extractScores(partId, records):
        if partId==0:
            next(records)
        import csv
        reader = csv.reader(records)
        for row in reader:
            if row[5] != '' and row[22] != '' and row[24] != '' and row[25] != '':
                (date, county, house_number, street_name, ) = (row[5], row[22], row[24], row[25])
                d = int(date.split('/')[2])
                temp = re.findall(r'\d+',house_number)
                res = list(map(int,temp))
                if len(res) > 0:
                    idd = findid(row[1], row[3], row[2])
                    if idd != None and (row[0] >= 2015 and row[0] <= 2019):
                        yield (idd, row[0])
    
    ticket = all_tickets.mapPartitionsWithIndex(extractScores)
    test = ticket.map(lambda x: ((x[0]), {x[0][1]: 1} )) \
    .reduceByKey(lambda x,y: (Counter(x) + Counter(y))) \
    .mapValues(lambda x: ([i for i in x.values()], len(x.keys()))) \
    .mapValues(lambda x: (x[0], (x[0][-1]- x[0][0])/ x[1]))
    
    test.saveAsTextFile('finale')
    
    
    