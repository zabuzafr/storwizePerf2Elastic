#!/usr/bin/python
import base64
import re
import json
import time
import paramiko
from time import gmtime, strftime

from elasticsearch import Elasticsearch
from datetime import datetime

version="1.0.1"

storwizeDataDir="/Users/pierrejacques/scripts/data"
elastic_server="http://localhost:9200"


if storwizeDataDir is None:
        print "Nmon files directory is not defined\n"
        exit(-1)

if elastic_server is None:
        print "Elastic server is not defined\n"
        exit(-2)

es=Elasticsearch()

storwize2ES=dict()

storwize2ES["version"]="1.0.1"

storwize2ES["copywhrite"]="(c) 2018  MIMIFIR Pierre-Jacques"
storwize2ES["email"]="pierrejacques.mimifir@shanaconsulting.fr"
storwize2ES["Comment"]="Open source script for IBM Storwize storage array"
index_name="storewize-lssystemstats"
dirs=os.listdir(storwizeDataDir)
for file_name in dirs:
	out=dict()
        file=storwizeDataDir + "/" + file_name
        fo=open(file)
        for line in fo:
		#ma=re.match("(\d+-\d+-\d+T\d+:\d+:\d+),\w+:\s+(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}),\w+:(\w+),\s+\w+:\w+=\d+,\s+\w+=(\w+),\w+:(\d+),\w+:(\d+)",line)
		ma=re.match("(^\d+|^\d+-\d+-\d+T\d+:\d+:\d+),\w+:\s+(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}),\w+:(\w+),\s+\w+:\w+=(\d+),\s+\w+=(\w+),\w+:(\d+),\w+:(\d+)",line)
		if ma is not None:
			if ma.group():
                		#t=line.rstrip().split(",")
				str_time=ma.group(4)
				dt=datetime.strptime(str_time,"%y%m%d%H%M%S")
				timestamp_str=dt.strftime("%Y-%m-%dT%H:%M:%S%Z")
				out["@timestamp"]=timestamp_str
				out["STORWIZE_IP"]=ma.group(2)
				out["STORWIZE_NAME"]=ma.group(3)
				out["METRIC_NAME"]=ma.group(5)
				out["VALUE"]=float(ma.group(6))
				out["PEAK"]=float(ma.group(7))
				#out["PEAK"]=float(ma.group(8))
				out["INFORMATION"]=storwize2ES
				#print json.dumps(out) + "\n"
				es.index(index=index_name.lower(),doc_type="storwize-lsystemstats-json",body=json.dumps(out))
