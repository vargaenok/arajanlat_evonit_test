import os
import psycopg2
import psycopg2.extras
import csv

from config import config


def database_loading(filename, file_list):
    rows = []
    which_database = filename.split("_")
    which_database = which_database[0]
    with open(f'csv_files/{filename}', 'r') as file:
        csvreader = csv.reader(file)
        company_row = next(csvreader)
        header = next(csvreader)
        print(header)
        print(company_row)
        target = get_target(company_row)
        for row in csvreader:
            if target == "Budapest":
                row.pop(-1)
                row.pop(-1)
                row.insert(0, "")
            row.append(company_row[0])
            row.append(target)
            rows.append(row)
    print(header)
    print(rows[0])
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        print("Iteration start")
        count = 0
        for row in rows:
            insert_query = f'INSERT INTO {which_database} (start_city_short,start_city,cont_20st,cont_40hc,cont_40st,company,target_city) VALUES (%s,%s,%s,%s,%s,%s,%s);'
            print(row)
            cur.execute(insert_query, row)

            conn.commit()
            count += 1
            print(count, 'Record inserted successfully into ' + which_database + ' table')
    except (Exception, psycopg2.Error) as error:
        print("Failed to insert data into database: ", error)
    finally:
        if conn:
            cur.close()
            conn.close()
            print("PostgreSQL connection is closed at \"data loading\"")


def get_target(company_row):
    if company_row[2] == "":
        target = "Budapest"
        return target
    target = company_row[2]
    target = target.split(": ")
    target = target[1]
    target = target.split(" (")
    target = target[0]
    return target


def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()

        # execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')


def creating_tables():
    print("\n")
    print("Creating tables for project...\n")
    commands = (
        """
        DROP table IF EXISTS road CASCADE;
        """,
        """
        DROP table IF EXISTS rail CASCADE;
        """,
        """
        DROP table IF EXISTS sea CASCADE;
        """,
        """
        CREATE TABLE sea (
            id serial primary key,
            start_city_short character varying(8),
            start_city character varying(90) NOT NULL,
            cont_20ST integer NOT NULL,
            cont_40ST integer NOT NULL,
            cont_40HC integer NOT NULL,
            company character varying(90) NOT NULL,
            target_city character varying(90) NOT NULL
        );
        """,
        """ 
        CREATE TABLE rail (
            id serial primary key,
            start_city_short character varying(8),
            start_city character varying(90) NOT NULL,
            cont_20ST integer NOT NULL,
            cont_40ST integer NOT NULL,
            cont_40HC integer NOT NULL,
            company character varying(90) NOT NULL,
            target_city character varying(90) NOT NULL
        );
        """,
        """
        CREATE TABLE road (
            id serial primary key,
            start_city_short character varying(8),
            start_city character varying(90) NOT NULL,
            cont_20ST integer NOT NULL,
            cont_40ST integer NOT NULL,
            cont_40HC integer NOT NULL,
            company character varying(90) NOT NULL,
            target_city character varying(90) NOT NULL
        );
        """)
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        # table one by one
        for command in commands:
            print("command started")
            cur.execute(command)
            print("command finished")
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def data_input():
    start = input("Starting Port: ")
    finish = input("Target City: ")
    container_type = input("Container type (20ST, 40ST, 40HC): ")
    profit_percent = float(input("Profit margin: "))
    usd_huf_change = float(input("USD/HUF exchange rate: "))
    print(start, finish, container_type, profit_percent, usd_huf_change)


def files_to_scan():
    print("\nChoose file to import: \n----------------------")
    list_o_files = os.listdir("csv_files")
    string_o_files = f'\n' .join(list_o_files)
    print(string_o_files)
    return list_o_files


if __name__ == '__main__':
    connect()
    creating_tables()
    while True:
        list_for_loading_all = files_to_scan()
        command = input("\nPlease give me the filename to scan (Q to finish loading): ")
        command = command.lower()
        if command == "q":
            break
        database_loading(command, list_for_loading_all)
    data_input()
    print("-------->PROGRAM ENDS HERE<--------")

