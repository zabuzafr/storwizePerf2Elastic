#!/usr/bin/python
import os,sys
import datetime
import base64
import re
import json
import time
import paramiko
import sched
import psycopg2
import getopt
import encodings
from datetime import date

dbconn=None
esconn=None
storconn=None
esHost=None

def help():
	print "Storwize Monitoring tool help"
	sys.exit(0)

def check_storwize_connection(host,user,password,keyfile):
	try:
		client = paramiko.SSHClient()
		client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		client.load_system_host_keys()
		client.connect(host,username=user,password=password)
		stdin,stdout,stderr = client.exec_command("echo")
		stdout.readlines()
		return 1
	except paramiko.ssh_exception.SSHException as msg:
		print msg
		return 0

def  get_array_data(host,user,passwd,keyfile):
	try:
		client = paramiko.SSHClient()
		client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
		client.load_system_host_keys()
		client.connect(host,username=user,password=passwd)
		xdict=dict()
		doc=dict()
		for command in "lssystem -delim \;" , "lssystemstats -delim \;":
			url=esHost 
			stdin,stdout,stderr = client.exec_command(command)
			dt= datetime.datetime.now().isoformat()
			d= datetime.date.today().isoformat()
			datas=[]
			for line in stdout:
				data=re.split(';',line)
				xdict[data[0]]=data[1] 
			key=command.split(' ')[0]
			doc['_id']=key + "__" + dt
			doc['document']=xdict
			doc['@timestamp']=dt
			doc['index']=key +"." + d
			print json.dumps(doc)
		#r = requests.post(url + index,data=xdict)
	except paramiko.ssh_exception.SSHException as msg:
		print msg

def main(argv):
	pgHost="localhost"
	pgPort="5432"
	pgUser=""
	pgPasswd=""
	pgDb=""
	conf="/etc/storewize"

	stHost=None
	stUser=None
	stPasswd=None
	stPrivateKeyFile=None

	esHost=None
	esPort=None
	esUser=None
	esPassword=None

	try:
		opts,args=getopt.getopt(argv,"h:p:u:f:u:P:d:f:g:h:j:k:x:c:b:n:o:",["pgHost=","pgPort=","pgUser=","pgPasswd=","pgDatabase=","configFile=","esHost=","esPort=","esUser=","esPasswd=","stHost=","stUser=","stPasswd=","stPrivateKeyFile="])

	except getopt.error as err:
		print "Command line error"
		help()
		print err
		sys.exit(-1)
	for opt, arg in opts:	
		print opt ,arg
		if opt in ("-h","--pgHost"):
			pgHost=arg
		elif opt  in ("-p","--pgPort"):
			pgPort=arg
		elif opt in ("-u","--pgUser"):
			pgUser=arg
		elif opt in ("-P","--pgPasswd"):
			pgPasswd=arg
		elif opt in ("-d","--pgDatabase"):
			pgDb=arg
		elif opt in ("-f","--confgFile"):
			conf=arg	
		elif opt in ("-H","--help"):
			help()
		elif opt in ("-g","--esHost"):
			esHost=arg
		elif opt in ("-h","--esPort"):
			esPort=arg
		elif opt in ("-j","--stHost"):
			stHost=arg
		elif opt in ("-k","--stUser"):
			stUser=arg
		elif opt in ("-x","--stPasswd"):
			stPasswd=arg
		elif opt in ("-c","--stPrivateKeyFile"):
			stPrivateKeyFile=arg

	if (pgHost is not None  and  pgPort is not None and pgUser is not None and pgPasswd is not None):
		try:
			conn=psycopg2.connect(database=pgDb,user=pgUser,password=pgPasswd,host=pgHost,port=pgPort)

		except psycopg2.OperationalError as msg:
			print 'Database connection error'
			print "\tHost:",pgHost
			print "\tUser:",pgUser
			print "\tPasswd:",pgPasswd
			print "\tPort:",pgPort
	if(esHost is not None and  esUser is not None and esPasswd):
		try:
			if(esHost is not None and esUser is not None):
				esconn=Elasticsearch(esHost)
		except Elasticsearch.ConnectionError as msg:
			print msg

	if(stHost is not None):
		print "Connection to Stowize Array"
		print "\tHost:",stHost	
		print "\tuser:",stUser
		print "\tpasswd:",stPasswd
		print "\tSSH private key file:",stPrivateKeyFile
	if check_storwize_connection(stHost,stUser,stPasswd,stPrivateKeyFile) == 1:
		get_array_data(stHost,stUser,stPasswd,stPrivateKeyFile) 

main(sys.argv[1:])
