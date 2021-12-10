#!/usr/bin/env python
# DataCenter Project
# mysql_access.py
# Store a file (any type of a binary file) to MySQL DB
# Retrieve a file (any type of a binary file) from MySQL DB

# For auth: put_user_pw <username> <pw> or get_pw <username>
# For Doc: put_doc <filename> or get_doc <id_md5>

# MySQL Access Codes largely from the site below
# https://pynative.com/python-mysql-blob-insert-retrieve-file-image-as-a-blob-in-mysql/

# Every binary file data is encode into base64 code
# Due to the complicated encoding/decoding problems 
# with data containing unicodes (e.g. docx files)

# MySQL library
import mysql.connector
from mysql.connector import Error

# Base64 Encoding/Decoding
import base64
import json

# MD5 Hahsing
import hashlib

import sys

# Global Variables
db_host = '34.134.156.106'
db_name = 'ocr_db'
# db_name = 'Electronics'
db_user = 'root'
db_password = 'csci-password'


# Insert username and password to auth table
def insert_user_pw(connection, username, password):
    print("Inserting username %s and password %s into auth table" % 
            (username, password))
    try:
        cursor = connection.cursor()
        sql_insert_blob_query = """ INSERT INTO auth
                          (username, password) VALUES (%s, %s)"""

        insert_text_tuple = (username, password)
        result = cursor.execute(sql_insert_blob_query, insert_text_tuple)
        connection.commit()
        print("username:%s and password:%s inserted successfully into auth table: %r" % 
                (username, password, result))
    except mysql.connector.Error as error:
        print("Failed inserting data into MySQL table {}".format(error))
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")


# Retrieve password of username from auth table for authentication
def get_user_pw(connection, username):
    print("Reading username %s's password from auth table" % (username))
    try:
        cursor = connection.cursor()
        sql_fetch_blob_query = """SELECT * from auth where username = %s"""

        get_blob_tuple = (username,)
        cursor.execute(sql_fetch_blob_query, get_blob_tuple)
        record = cursor.fetchall()

        if (len(record) > 1):
            print("There are more than one record (%d) for %s, %r" %
                     (len(record), username, record))
        else:
            print("record for %s, %r" % (username, record))
        password = ''
        for row in record:
            print("username = %s, password = %s" % (row[0], row[1]))
            password = password + row[1]
    except mysql.connector.Error as error:
        print("Failed to read BLOB data from MySQL table {}".format(error))
        password = ''
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
        return password


def read_with_filename(filename):
    with open(filename, 'rb') as file:
        binary_data = file.read()
    return binary_data


# Insert input BLOB (file binary data) into MySQL doc DB Table
def insert_doc_file(connection, bin_data):
    print("Inserting data into doc table")
    try:
        cursor = connection.cursor()
        sql_insert_blob_query = """ INSERT INTO doc
                          (username, documentId, labels, safeSearch, filename) VALUES (%s, %s, %s, %s, %s)"""
        username = 'akhil'
        id = hashlib.md5(bin_data).hexdigest()
        filename = "dog" + ".jpg"
        print(id)
        content = [{
                'mid': "/m/01yrx",
                'description': "Giraffe",
                'score': 0.9487507939338684,
                'topicality': 0.9487507939338684
                },{
                'mid': "/m/014sv8",
                'description': "Eye",
                'score': 0.939929723739624,
                'topicality': 0.939929723739624
                },{
                'mid': "/m/0307l",
                'description': "Felidae",
                'score': 0.9094058871269226,
                'topicality': 0.9094058871269226
                },{
                'mid': "/m/01lrl",
                'description': "Carnivore",
                'score': 0.9046433568000793,
                'topicality': 0.9046433568000793
                }]
        
        content1 = {'adult': 'VERY_UNLIKELY', 'medical': 'VERY_UNLIKELY', 'spoofed': 'VERY_UNLIKELY', 'violence': 'VERY_UNLIKELY', 'racy': 'VERY_UNLIKELY'}
        content1 = json.dumps(content1)

        content = json.dumps(content)

        # Convert data into tuple format
        insert_blob_tuple = (username, id, content, content1, filename)
        result = cursor.execute(sql_insert_blob_query, insert_blob_tuple)
        connection.commit()
        print("Doc file data (id:%s) inserted successfully into doc table: %r" % 
                (id, result))
    except mysql.connector.Error as error:
        print("Failed inserting data into MySQL table {}".format(error))
    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")

def save_as_filename(bin_data, filename):
    # Store binary data on file system
    with open(filename, 'wb') as file:
        file.write(bin_data)


def get_doc_file(connection, id, name, username):
    print("Reading data from a doc table")
    binary_data = b''
    try:
        cursor = connection.cursor()
        sql_fetch_blob_query = """SELECT * from doc where username = %s AND documentId = %s"""
        get_blob_tuple = (username,id,)
        cursor.execute(sql_fetch_blob_query, get_blob_tuple)
        record = cursor.fetchall()
        print('testing DB')
        labels = json.loads(record[0][2]) #first record
        print(labels)

        sql_query = """SELECT * from doc
                WHERE username = %s AND JSON_CONTAINS(labels,'{"description": """+ '"' + name + '"' + """}')"""
        get_blob_tuple = (username,)
        cursor.execute(sql_query, get_blob_tuple)
        record = cursor.fetchall()
        print('Select label query')
        labels = json.loads(record[0][3]) # first record
        print(record[0][4])
        print(labels)
    except mysql.connector.Error as error:
        print("Failed to read BLOB data from MySQL table {}".format(error))
        binary_data = None

    finally:
        if (connection.is_connected()):
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
        return binary_data

# START main
def main():
    global db_host
    global db_name
    global db_user
    global db_password

    username = ''
    password = ''

    # Get Arguments
    if len(sys.argv) < 3:
        print ("Usage: mysql_access.py <Function: put_user_pw | get_pw \
            | put_doc | get_doc\n")
        sys.exit(1)
    print (sys.argv)
    command = sys.argv[1]

    try:
        connection = mysql.connector.connect(host=db_host,
                                             database=db_name,
                                             user=db_user,
                                             password=db_password)
    except mysql.connector.Error as error:
        print("Error Connecting to MySQL DB {}".format(error))

    # Parse command line
    # Insert username and password to auth table
    if command == 'put_user_pw':
        username = sys.argv[2]
        password = sys.argv[3]
        insert_user_pw(connection, username, password)

    # Retrieve password of username from auth table for authentication
    elif command == 'get_pw':
        username = sys.argv[2]
        password = get_user_pw(connection, username)
        print ("username: %s: password: %s" % (username, password))

    # Insert a doc file's content to doc table
    elif command == 'put_doc':
        doc_name = sys.argv[2]
        # If input is not a filename but a binary_data,
        # Remove (Comment out) bin_data = read_with_filename(filename)
        bin_data = read_with_filename(doc_name)
        insert_doc_file(connection, bin_data)

    # Retrieve a doc file's content from doc table with doc_id
    # doc_id is md5 hash of a doc file's content
    elif command == 'get_doc':
        doc_id = sys.argv[2]
        username = sys.argv[3]
        bin_data = get_doc_file(connection, 'e6f6d6397cb702596fe07a6870cd9924', doc_id, username)
        filename = doc_id
        # If output is not a filename but a binary_data,
        # Remove (Comment out) bin_data = save_as_filename(bin_data, filename)
        save_as_filename(bin_data, filename)
    else:
        print ("Usage: mysql_access.py <command: put_user_pw | get_pw \
            | put_doc | get_doc\n")
        sys.exit(1)

    # Create Table: Used for testing purpuses
    # createTable(hostname, dbname, username, pw)
    # createTable(db_host, db_name, db_user, db_password)
# END main


if __name__ == '__main__':
    main()