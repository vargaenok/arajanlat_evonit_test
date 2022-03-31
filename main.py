import os
import psycopg2
import psycopg2.extras
import csv

from config import config


def database_loading(filename):
    rows = []
    with open(f'csv_files/{filename}', 'r') as file:
        csvreader = csv.reader(file)
        company_row = next(csvreader)
        header = next(csvreader)
        target = get_target(company_row)
        for row in csvreader:
            row.append(company_row[0])
            row.append(target)
            print(row)
            rows.append(row)
    print(company_row)
    print(company_row[0])
    print(header)
    print(rows)


def get_target(company_row):
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


if __name__ == '__main__':
    connect()
    files_to_scan()
    # input("\nPlease give me the filename to scan: ")
    database_loading(filename='sea_penguin_deham.csv') # visszaírni sima változóra
    data_input()
    print("-------->PROGRAM ENDS HERE<--------")

