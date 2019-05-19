'''
Script to get DB schema provided the DB details like
DB_HOST
DB_PORT
DB_USER
DB_PASSWORD
DB_NAME
in environment
'''
import os
import sys
import json
import pymysql.cursors
from collections import OrderedDict
import re

class ConfigContext:
    '''
    Global configs
    '''
    def __init__(self):
        '''
        Initialize global configs.
        '''
        self.db_host = self.environ_or_die('DB_HOST')
        self.db_port = self.environ_or_die('DB_PORT')
        self.db_user = self.environ_or_die('DB_USER')
        self.db_password = self.environ_or_die('DB_PASSWORD')
        self.db_name = self.environ_or_die('DB_NAME')


    def environ_or_die(self, env):
        '''
        Get the variable from environment or die
        '''
        try:
            return os.environ[env]
        except:
            print('Missing {envvar} in environment.'.format(envvar=env))
            sys.exit(1)

CONFIG = ConfigContext()

def get_schema(table_name):
    '''
    Get Schema in json format
    '''
    table_schema = []
    connection = pymysql.connect(host=CONFIG.db_host,
                                 user=CONFIG.db_user,
                                 password=CONFIG.db_password,
                                 db=CONFIG.db_name,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    with connection.cursor() as cursor:
        sql = "DESC {table}".format(table=table_name)
        cursor.execute(sql)
        result = cursor.fetchall()
        for each_column in result:
            table_schema.append((each_column['Field'],each_column['Type']))

    return table_schema

def get_tabes():
    '''
    Get tables
    '''
    result = ''
    tables = []
    connection = pymysql.connect(host=CONFIG.db_host,
                                 user=CONFIG.db_user,
                                 password=CONFIG.db_password,
                                 db=CONFIG.db_name,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.Cursor)
    with connection.cursor() as cursor:
        sql = "SHOW tables"
        cursor.execute(sql)
        result = cursor.fetchall()
        #result = cursor.fetchall_unbuffered()
        #connection.commit()
        for t in result:
            tables.append(t[0])

    return tables

def main():
    '''
    Main function
    '''
    config_json = {}
    config_json[CONFIG.db_name] = {}
    tables = get_tabes()
    for t_name in tables:
        config_json[CONFIG.db_name][t_name] = {}
        config_json[CONFIG.db_name][t_name]['columns'] = []
        config_json[CONFIG.db_name][t_name]['schema'] = {}
        t_schema = get_schema(t_name)
        for column in t_schema:
            config_json[CONFIG.db_name][t_name]['columns'].append(column[0])
            if re.match(r'.*?datetime.*?', column[1]):
                config_json[CONFIG.db_name][t_name]['schema'][column[0]] = 'timestamp'
            elif re.match(r'.*?int.*?unsigned.*?', column[1]):
                config_json[CONFIG.db_name][t_name]['schema'][column[0]] = 'long'
            elif re.match(r'.*?int.*?', column[1]):
                config_json[CONFIG.db_name][t_name]['schema'][column[0]] = 'int'
            elif re.match(r'.*?date.*?', column[1]):
                config_json[CONFIG.db_name][t_name]['schema'][column[0]] = 'date'
            elif re.match(r'.*?char.*?', column[1]):
                config_json[CONFIG.db_name][t_name]['schema'][column[0]] = 'string'
            elif re.match(r'.*?text.*?', column[1]):
                config_json[CONFIG.db_name][t_name]['schema'][column[0]] = 'string'
            elif re.match(r'.*?double.*?', column[1]):
                config_json[CONFIG.db_name][t_name]['schema'][column[0]] = 'float'
            elif re.match(r'.*?decimal\(.*?\).*?', column[1]):
                decimal_vars_match = re.match(r'.*?decimal\((.*?)\).*?', column[1])
                decimal_vars = decimal_vars_match.group(1).split(',')
                config_json[CONFIG.db_name][t_name]['schema'][column[0]] = 'decimal({0},{1})'.format(int(decimal_vars[0]), int(decimal_vars[1]))
            else:
                config_json[CONFIG.db_name][t_name]['schema'][column[0]] = 'string'

    print(json.dumps(config_json))

if __name__ == '__main__':
    main()
