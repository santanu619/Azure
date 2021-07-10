import pyodbc 
import logging
import textwrap
from decouple import config

'''
@Author: Santanu Mohapatra
@Date: 08/07/2021
@Last Modified by: Santanu Mohapatra
@Last Modified Time: 20:30 PM
@Title: Perform CRUD operations with pyodbc in azure
'''

class CRUD_SQL_Database:
    '''
    Class: CRUD
    Description: To perform CRUD Operation
    Functions:
        createConnection()
        create_table()
        insert_data()
        retreive_data()
    Variable: None
    '''
    def __init__(self):
        self.__server = config('DB_SERVER')
        self.__username = config('DB_USERNAME')
        self.__password = config('DB_PASSWORD')
        self.__database_name = config('DB_NAME')
        self.__driver = config('DB_DRIVER')
        self.createConnection()

    def createConnection(self):
        '''
        Description: To Create Connection to the SQL Server
        Parameter: None
        Return: None
        '''
        try:
            connection_string = textwrap.dedent('''
                Driver={driver};
                Server={server};
                Database={database};
                Uid={username};
                Pwd={password};
                Encrypt=yes;
                TrustServerCertificate=No;
                Connection Timeout=30;
                '''.format(driver=self.__driver, server=self.__server, database=self.__database_name,
                            username=self.__username, password=self.__password))
            connection = pyodbc.connect(connection_string)
            if (connection):
                logging.info("Connection Successful!")
                
            self.__cnxn = connection
        except Exception:
            logging.exception("Connection Unsuccessful!")
    
    def create_table(self):
        '''
        Description: To Create Table in SQL Database
        Parameter: None
        Return: None
        '''
        try:
            cursor = self.__cnxn.cursor()
            cursor.execute('CREATE TABLE employee (id int, emp_name nvarchar(50), salary float);')
            self.__cnxn.commit()
            logging.info("Successfully Created Table")
        except Exception:
            logging.exception("Create Table Aborted")

    def insert_data(self):
        '''
        Description: To Insert Data in SQL Database
        Parameter: None
        Return: None
        '''
        try:
            cursor = self.__cnxn.cursor()
            cursor.execute("INSERT INTO employee (id, emp_name, salary) VALUES(1,'Santanu',60000);")
            self.__cnxn.commit()
            logging.info("Successfully Inserted Data")
        except Exception:
            logging.exception("Insert Data Aborted")

    def retreive_data(self):
        '''
        Description: To Retreive Data in SQL Database
        Parameter: None
        Return: None
        '''
        try:
            cursor = self.__cnxn.cursor()
            cursor.execute('SELECT * FROM employee;')
            result=cursor.fetchall()
            for data in result:
                logging.info(data)
            logging.info("Successfully Retreived Data")
        except Exception:
            logging.exception("Retreive Data Aborted")


if __name__ == '__main__':
    obj = CRUD_SQL_Database()
    obj.create_table()
    obj.insert_data()
    obj.retreive_data()