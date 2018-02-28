# Purpose : Main file to process the input file and load the data into the database
# Author : Kasirajan Pothiraj
# Date : Feb 2018

from Db import Db
import utils as u1
import csv


# Read and process the config file
print ("Reading Config File.........")
cfg = {}
with open("config.properties") as f:
    for line in f:
       (key, val) = line.split('=')
       cfg[key] = val.replace("\n","")

print ("List of Config Values \n")

for keys,values in cfg.items():
    print(keys, values)

print ("Processing the input file .....")

u1.replace_word(cfg['ip_file'], cfg['op_file'], "^&^", ",")

db = Db(username=cfg['db_user'], password=cfg['db_passwd'], database=cfg['db_name'], driver=cfg['db_engine'])

ret = []
ret = db.select(cfg['tbl_name'], columns='*')
print("Currently Number of records in table :", len(ret))

if (len(ret) > 0):
    print ("Truncating the table : ", cfg['tbl_name'])
    db.truncate(cfg['tbl_name'])

sql = 'INSERT INTO %s(%s) VALUES (%s)' % (self.enclose_sys(table), ','.join(cols), ','.join(['%s'] * len(vals)))

query = 'LOAD DATA INFILE (%s) INTO (%s) FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;' %(cfg['op_file'], ','.join(cfg['tbl_name'])),' FIELDS TERMINATED BY ',' ENCLOSED BY '"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES;'

with open(cfg['op_file'], 'r') as f:
    reader = csv.reader(f)
    next(reader)  # Skip the header row.
    for row in reader:
        print(row)
        db.insert(cfg['tbl_name'], row)
        print("Row completed " , row)
        #next(reader)


#close the connection to the database.
db.commit()
db.disconnect()
print "Done"


'''
print ("Testing")
db = Db(username='dba', password='password123', database='ref_data', driver='mysql')
ret = []
ret = db.select(table='geo_info', columns='*')


print(len(ret))

'''
