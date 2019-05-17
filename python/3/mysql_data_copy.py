#!/usr/bin/env python3

# Imports
from __future__ import print_function
from datetime import datetime
import argparse
import getpass
import re
import subprocess
import time

# Functions
def ts_tag():
    ts = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    return ("[" + ts + "]")

# Algorithm
print("############################\n## MYSQL DATA COPY SCRIPT ##\n############################\n")

## Variable initialization
print("[INFO]" + ts_tag() + " Initializing...")
src_host = src_user = src_pswd = None
trg_host = trg_user = trg_pswd = None
src_port = trg_port = 3306
no_create_db = no_create_info = None
databases = None


## Creating and populating parser
parser = argparse.ArgumentParser()
parser.add_argument("--source-database",dest="src_host",help="Source database hostname")
parser.add_argument("--source-port",dest="src_port",help="Source database port",default=3306)
parser.add_argument("--source-user",dest="src_user",help="Source database user")
parser.add_argument("--source-pswd",dest="src_pswd",help="Source database password")
parser.add_argument("--target-database",dest="trg_host",help="Target database hostname")
parser.add_argument("--target-port",dest="trg_port",help="Target database port",default=3306)
parser.add_argument("--target-user",dest="trg_user",help="Target database user")
parser.add_argument("--target-pswd",dest="trg_pswd",help="Target database password")
parser.add_argument("--databases",dest="databases",help="Schemas to copy",default=None)
parser.add_argument("--create-db",dest="create_db",action="store_true",help="Keep existing databases/schemas")
parser.add_argument("--create-info",dest="create_info",action="store_true",help="Keep existing tables")
parser.add_argument("-v","--verbose",dest="verbosity",action="count",help="Verbosity switch",default=0)

## Parsing arguments
print("[INFO]" + ts_tag() + " Parsing arguments... ",end=" ")
args = parser.parse_args()
print("OK!")

## Check for missing parameters
print("[INFO]" + ts_tag() + " Checking for missing arguments...")
if args.verbosity > 0: print("[VERBOSE]" + ts_tag() + " Checking source host...",end=" ")
if not args.src_host:
    if args.verbosity > 0: print("NOT FOUND!")
    args.src_host = str(input('[INPUT]'+ ts_tag() + ' Please enter the source hostname: '))
else:
    if args.verbosity > 0: print("OK!")
if args.verbosity > 0: print("[VERBOSE]" + ts_tag() + " Checking source user...",end=" ")
if not args.src_user:
    if args.verbosity > 0: print("NOT FOUND!")
    args.src_user = str(input('[INPUT]'+ ts_tag() + ' Please enter the user for ' + args.src_host + ': '))
else:
    if args.verbosity > 0: print("OK!")
if args.verbosity > 0: print("[VERBOSE]" + ts_tag() + " Checking source password...",end=" ")
if not args.src_pswd:
    if args.verbosity > 0: print("NOT FOUND!")
    args.src_pswd = str(getpass.getpass(prompt='[INPUT]'+ ts_tag() + ' Please enter the password for ' + args.src_user + ' on ' + args.src_host + ': '))
else:
    if args.verbosity > 0: print("OK!")
if args.verbosity > 0: print("[VERBOSE]" + ts_tag() + " Checking target hostname...",end=" ")
if not args.trg_host:
    if args.verbosity > 0: print("NOT FOUND!")
    args.trg_host = str(input('[INPUT]'+ ts_tag() + ' Please enter the target hostname: '))
else:
    if args.verbosity > 0: print("OK!")
if args.verbosity > 0: print("[VERBOSE]" + ts_tag() + " Checking target user...",end=" ")
if not args.trg_user:
    if args.verbosity > 0: print("NOT FOUND!")
    args.trg_user = str(input('[INPUT]'+ ts_tag() + ' Please enter the user for ' + args.trg_host + ': '))
else:
    if args.verbosity > 0: print("OK!")
if args.verbosity > 0: print("[VERBOSE]" + ts_tag() + " Checking target password...",end=" ")
if not args.trg_pswd:
    if args.verbosity > 0: print("NOT FOUND!")
    args.trg_pswd = str(getpass.getpass(prompt='[INPUT]'+ ts_tag() + ' Please enter the password for ' + args.trg_user + ' on ' + args.trg_host + ': '))
else:
    if args.verbosity > 0: print("OK!")

## Check database connectivity
print("[INFO]" + ts_tag() + " Checking source database connectivity...")
if args.verbosity > 0: print("[VERBOSE]" + ts_tag() + " Building mysql command to be run with subprocess")
conn_test = 'mysql --user="' + args.src_user + '" --password=\'' + args.src_pswd + '\' --host="' + args.src_host + '" --execute="SELECT \'Connected successfully\'" --batch --silent'
if args.verbosity > 1: print("[VERBOSE]" + ts_tag() + " Command: " +  re.sub('--password=\'[A-Za-z0-9!#$_]+\'','--password=\'XXXXXXXXXXXXXXXXXXXXX\'',str(conn_test)))
check = subprocess.Popen(conn_test,shell=True,stdout=subprocess.PIPE)
if check.communicate() == 'Connected successfully':
    print("[INFO]" + ts_tag() + " Source database connectivity OK")
check.stdout.close()

print("[INFO]" + ts_tag() + " Checking target database connectivity...")
if args.verbosity > 0: print("[VERBOSE]" + ts_tag() + " Building mysql command to be run with subprocess")
conn_test = 'mysql --user="' + args.trg_user + '" --password=\'' + args.trg_pswd + '\' --host="' + args.trg_host + '" --execute="SELECT \'Connected successfully\'" --batch --silent'
if args.verbosity > 1: print("[VERBOSE]" + ts_tag() + " Command: " +  re.sub('--password=\'[A-Za-z0-9!#$_]+\'','--password=\'XXXXXXXXXXXXXXXXXXXXX\'',str(conn_test)))
check = subprocess.Popen(conn_test,shell=True,stdout=subprocess.PIPE)
if check.communicate() == 'Connected successfully':
    print("[INFO]" + ts_tag() + " Target database connectivity OK")

## Check source schemas
print("[INFO]" + ts_tag() + " Checking schemas in source database...")
if not args.databases:
    print("[INFO]" + ts_tag() + " No schemas were provided through command line")
    schemas_command = 'mysql --user="' + args.trg_user + '" --password="' + args.trg_pswd + '" --host="' + args.trg_host + '" --execute="SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN (\'mysql\',\'sys\',\'information_schema\',\'performance_schema\')" --batch --silent'
    output = subprocess.Popen(schemas_command,shell=True,stdout=subprocess.PIPE)
else:
    for item in str(args.databases).split(' '):
        schemas_sql = ""
        schemas_sql += "'" + item + "',"
        schemas_sql.rstrip()
    schemas_command = 'mysql --user="' + args.trg_user + '" --password="' + args.trg_pswd + '" --host="' + args.trg_host + '" --execute="SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ("' + schemas_sql + '")" --batch --silent'
    output = subprocess.Popen(schemas_command,shell=True,stdout=subprocess.PIPE)
schema_list = output.communicate()
print("[INFO]" + ts_tag() + " Creating schema list...",end=' ')
args.databases = ""
for item in schema_list:
    args.databases += str(item)
print('Done!')

## Build mysqldump command

dump_command = 'mysqldump'
dump_command += ' --column-statistics=0'
dump_command += ' --user=\'' + str(args.src_user) + '\''
dump_command += ' --password=\''+ str(args.src_pswd) + '\''
if not args.create_db:
    answer = input('[INPUT]' + ts_tag() + ' Not creating schemas. Is that correct? [Y/n]: ')
    if  answer == 'Y' or answer == 'y':
        dump_command += ' --no-create-db'
if not args.create_info:
    answer = input('[INPUT]' + ts_tag() + ' Not creating tables. Is that correct? [Y/n]: ')
    if  answer == 'Y' or answer == 'y':
        dump_command += ' --no-create-info'
if not args.databases:
    dump_command += ' --all-databases'
else:
    dump_command += ' --databases ' + str(args.databases).replace(',',' ').replace('\\n',' ').replace("b'","").replace("'None","")
if args.src_port != 3306:
    dump_command += ' --port=' + str(args.src_port)
dump_command += ' --host=' + str(args.src_host)
## Build Import command
load_command = 'mysql'
load_command += ' --user=\'' + str(args.trg_user) + '\''
load_command += ' --password=\''+ str(args.trg_pswd) + '\''
load_command += ' --host=\'' + str(args.trg_host) + '\''
if args.trg_port != 3306:
    load_command += ' --port=' + str(args.src_port)

## Review Commands
print("\n")
print("[CHECK]" + ts_tag() + ' Dump command: ' + re.sub('--password=\'[A-Za-z0-9!#$_]+\'','--password=\'XXXXXXXXXXXXXXXXXXXXX\'',str(dump_command)))
print('-------------------------------------')
print("[CHECK]" + ts_tag() + ' Load command: ' + re.sub('--password=\'[A-Za-z0-9!#$_]+\'','--password=\'XXXXXXXXXXXXXXXXXXXXX\'',str(load_command)))
print('--\n')
print('[INPUT]' + ts_tag() + " Review the commands above and press 'Y' to proceed or any other key to abort:",end=" ")
answer = input()
if answer == 'Y' or answer == 'y':
    ## Copy process
    print('[NOTICE]' + ts_tag() + " Starting data copy process (Piped Dump + Load)")
    ### Dump the schemas
    dump = subprocess.Popen(dump_command,shell=True,stdout=subprocess.PIPE)
    ### Import into the new database using a pipe
    load = subprocess.Popen(load_command,shell=True,stdin=dump.stdout,stdout=subprocess.PIPE)
    ### Close the pipe
    dump.stdout.close()
    ### Get output
    load.communicate()
    print('[NOTICE]' + ts_tag() + " Data copy complete!")
else:
    print('[NOTICE]' + ts_tag() + " Data copy CANCELLED BY USER!")
    
