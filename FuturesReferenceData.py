import pandas as pd
from pandas import DataFrame, Series
import requests
from bs4 import BeautifulSoup
import MySQLdb as mdb

def initiate_db(cur):

	cur.execute('CREATE DATABASE IF NOT EXISTS futures_master')

	cur.execute('USE futures_master')
	cur.execute(
		'CREATE TABLE IF NOT EXISTS `reference_data` ('
		'`id` int NOT NULL AUTO_INCREMENT,'
		'`symbol` varchar(32) NOT NULL,'
		'`name` varchar(64) NOT NULL,'
		'`exchange` varchar(32) NOT NULL,'
		'`months` varchar(32) NOT NULL,'
		'`point_val` int(16) NOT NULL,'
		'PRIMARY KEY (`id`)'
		') ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8'
		)

def gather_data():
	#converts site's xml code to text
	site = requests.get('http://www.barchart.com/futures/specifications.php')
	data = site.text
	
	#parse for reference data
	soup = BeautifulSoup(data)

	tag_table = soup('tr', height='30\\')
	
	#stores values in a Python list of lists
	specs = []

	for tag_lines in tag_table:
		contract = [ 
		str(tag_lines('td')[0].string),
		str(tag_lines('td')[1].string),
		str(tag_lines('td')[2].string),
		str(tag_lines('td')[5].string),
		str(tag_lines('td')[8].string).replace("$", "").replace(",", "")]
		specs.append(contract)

	names = ['symbol','name', 'exchange', 'months', 'point_val']
	
	
	return DataFrame(specs, columns=names).set_index('symbol')
	



if __name__ == "__main__":
	
	db_host = 'localhost'
	db_user = 'futures'
	db_passwd = 'password'

	db = mdb.connect(host=db_host, user=db_user, passwd=db_passwd)
	cur = db.cursor()
	
	initiate_db(cur)
	ref_data = gather_data()
	#The 'mysql' flavor with DBAPI connection is deprecated and will be removed in future versions
	ref_data.to_sql(con=db, name='reference_data', flavor='mysql',if_exists='append')
