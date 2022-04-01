import os
import psycopg2
import psycopg2.extras
import csv

from config import config


def database_loading(incoming_command, file_list):
    if incoming_command == "load_all":
        for single_filename in file_list:
            csv_handling(single_filename)
        print("all .csv files are loaded")
    else:
        csv_handling(incoming_command)


def csv_handling(filename):
    rows = []
    hungarian = 0
    which_database = filename.split("_")
    which_database = which_database[0]
    with open(f'csv_files/{filename}', 'r') as file:
        csvreader = csv.reader(file)
        company_row = next(csvreader)
        header = next(csvreader)
        target, hungarian = get_target(company_row)
        for row in csvreader:
            if hungarian == 1:
                row.pop(-1)
                row.pop(-1)
                row.insert(0, "")
            row.append(company_row[0])
            row.append(target)
            rows.append(row)
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        print("Iteration start")
        count = 0
        for row in rows:
            insert_query = f'INSERT INTO {which_database} (start_city_short,start_city,cont_20st,cont_40hc,cont_40st,company,target_city) VALUES (%s,%s,%s,%s,%s,%s,%s);'
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
        magyar = 1
        return target, magyar
    target = company_row[2]
    target = target.split(": ")
    target = target[1]
    target = target.split(" (")
    target = target[0]
    magyar = 0
    return target, magyar


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
            cur.execute(command)
        cur.close()
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def data_input():
    start = input("Starting Port: ")
    finish = " " + input("Target City: ")
    while True:
        container_type = input("Container type (20ST, 40ST, 40HC): ")
        container_type = container_type.upper()
        print(container_type)
        if container_type == "20ST":
            container_type_for_sql = "cont_20st"
            break
        elif container_type == "40ST":
            container_type_for_sql = "cont_40st"
            break
        elif container_type == "40HC":
            container_type_for_sql = "cont_40hc"
            break
        else:
            print("Please give a valid container type!")
    profit_percent = float(input("Profit margin: "))
    usd_huf_change = float(input("USD/HUF exchange rate: "))
    list_of_routes = data_handling(start, finish, container_type_for_sql)
    final_list(list_of_routes, container_type, profit_percent, usd_huf_change)


def final_list(routes, container_type, profit, exchange):
    routes_as_list = []
    list_for_sort = []
    for route in routes:
        routes_as_list.append(list(route))
    for list_route in routes_as_list:
        usd_sum = list_route[6] + list_route[7]
        part_sum = usd_sum * exchange  # usd resz huf-ban
        overall_sum = part_sum + list_route[8]
        list_route.append(overall_sum)
        list_for_sort.append(list_route)
    list_for_sort = sorted(list_for_sort, key=lambda x:x[-1], reverse=False)
    print(f'With the the container type as {container_type}, and with {profit}% profit,')
    print(f'these are the routes it can take. (CHEAPEST on TOP!):')
    for row in list_for_sort:
        cost_with_profit = row[-1] / 100 * profit + row[-1]
        print(f' {row[4]} ({row[5]}) ---> {row[2]} ({row[3]}) ---> {row[0]} ({row[1]}). Flat cost: {row[-1]} HUF. With Profit: {cost_with_profit} HUF')


def data_handling(start, finish, containertype):
    conn = None
    try:
        params = config()

        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)

        # create a cursor
        cur = conn.cursor()
        first_part = f'select ro.start_city as target_city, ro.company, ra.start_city as transit, ra.company, se.start_city as origin, se.company, se.{containertype}, ra.{containertype}, ro.{containertype}'
        second_part = """
                        from road ro 
                        inner join rail ra on ro.target_city = ra.target_city
                        inner join sea se on ra.start_city=se.target_city
                        """
        third_part = f' where ro.start_city = \'{finish}\' AND se.start_city = \'{start}\''
        forth_part = f' group by ro.start_city, ro.company, ra.start_city, ra.company, se.start_city, se.company, se.{containertype}, ra.{containertype}, ro.{containertype}'
        postgresql_select_query = first_part + second_part + third_part + forth_part

        # execute a statement
        cur.execute(postgresql_select_query)

        sql_list = cur.fetchall()

    except (Exception, psycopg2.Error) as error:
        print("Error while fetching data from PostgreSQL", error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
    return sql_list


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
        print("\nPlease give me the filename to import, or...")
        command = input("input \"Q\" to finish loading, or \"load_all\" to load all files: ")
        command = command.lower()
        if command == "q":
            break
        elif command == "load_all":
            database_loading(command, list_for_loading_all)
            break
        database_loading(command, list_for_loading_all)
    data_input()
    print("-------->PROGRAM ENDS HERE<--------")

