import os
import psycopg2
import time
import logging

sql_dir = '/folder_with_sql_files/'
sql_files = [
    'list_of_sql_files',
]
pre_commands = []

rs_creds = {
    'database': os.environ.get('RS_DATABASE'),
    'user': os.environ.get('RS_USER'),
    'host': os.environ.get('RS_HOST'),
    'port': os.environ.get('RS_PORT'),
    'password': os.environ.get('RS_PASSWORD'),
}
db = psycopg2.connect(**(rs_creds))
db.set_session(readonly=False, autocommit=True)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

for pre_command in pre_commands:
    logger.info(' executing "%s"...' % pre_command)
    st = time.time()
    cursor = db.cursor()
    try:
        cursor.execute(pre_command)
    except:
        logger.info('     ...error raised in %.2f sec' % (time.time() - st))        
        raise
    cursor.close()
    logger.info('     ...finished in %.2f sec' % (time.time() - st)) 
for sql_file in sql_files:
    with open(os.path.join(sql_dir, sql_file + '.sql'), 'r') as infile:
        query = infile.read()
    logger.info(' executing %s...' % sql_file)
    st = time.time()
    cursor = db.cursor()
    try:
        cursor.execute(query)
    except:
        logger.info('     ...error raised in %.2f sec' % (time.time() - st))
        logger.info(query)
        raise
    cursor.close()
    logger.info('     ...finished in %.2f sec' % (time.time() - st))