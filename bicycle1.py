import psycopg2
import matplotlib.pyplot as plt
from tabulate import tabulate
import numpy as np

database_info = psycopg2.connect(
    database='mis',
    user='postgres',
    password='amir',
    host='localhost'
)


def create_database():
    from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

    con = database_info

    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    cur = con.cursor()

    sql = "create database mis"

    cur.execute(sql)

    con.commit()

    print("database created")

    cur.close()

    con.close()


def create_table():
    con = database_info

    # customers table
    cur = con.cursor()

    sql = "create table customers (National_ID int unique not null, F_name varchar(20), L_name varchar(20)" \
          ", Birth_year int not null, Register_date DATE NOT NULL DEFAULT CURRENT_DATE, Gender BOOLEAN);"

    cur.execute(sql)

    con.commit()

    print("customers table created")

    # prices table
    sql = "create table prices (ID serial unique not null primary key, price_year DATE UNIQUE NOT NULL DEFAULT CURRENT_DATE," \
          " price_per_minute int, cancelation_fee int);"

    cur.execute(sql)

    con.commit()

    print("prices table created")

    # stations table
    sql = "create table stations (ID serial unique not null primary key, Station_name varchar(25) unique, latitude int, longitude int," \
          "Street_name varchar(25), capacity int, Available_bicycle int );"

    cur.execute(sql)

    con.commit()

    print("stations table created")

    # Trips table
    sql = "create table Trips (ID serial unique not null, customer int,origin int, destination int, alt_destination int, alt_origin int,date_tp DATE NOT NULL DEFAULT CURRENT_DATE ," \
          "foreign key(origin) references stations (ID), " \
          "foreign key(customer) references customers (national_ID)," \
          "confirmation_des2 BOOLEAN, confirmation_org2 BOOLEAN, duration int, trip_conf int   );"

    cur.execute(sql)

    con.commit()

    print("trips table created")

    # close
    cur.close()

    con.close()


def insert_prices():
    con = database_info
    cur = con.cursor()

    print('\n---- inserting data in prices table ----')
    print('\ndata entry format: (year[YYYY],price per minute,cancelation fee)')
    data = input('\nplease enter the data in the given format: ')
    text = data.split(',')
    text[0] = "01/01/" + text[0]
    sql = "insert into prices (price_year, price_per_minute, cancelation_fee)" \
          " values ('" + text[0] + "', '" + text[1] + "', '" + text[2] + "' )"

    cur.execute(sql)

    con.commit()

    print(" insert done! ")

    cur.close()
    tables()
    # con.close()


def insert_stations():
    con = database_info
    cur = con.cursor()

    print('\n---- inserting data in stations table ----')
    print('\ndata entry format: (station name,latitude,longitude,street name,capacity,available bicycles)')

    data = input('\nplease enter the data in the given format: ')
    text = data.split(',')

    sql = "insert into stations (station_name, latitude, longitude, street_name, capacity, Available_bicycle)" \
          " values ('" + text[0] + "', '" + text[1] + "', '" + text[2] + "', '" + text[3] + "', '" + text[4] + "', '" + \
          text[5] + "' )"

    cur.execute(sql)
    con.commit()

    print(" insert done! ")

    cur.close()
    tables()
    con.close()


def insert_customers():
    con = database_info
    cur = con.cursor()

    print('\n---- inserting data in customers table ----')
    print(
        '\ndata entry format: (First name,Last name,National ID,Birth year[YYYY],Register date[MM/DD/YYYY],Gender (0>>Male - 1>>Female))')

    data = input('\nplease enter the data in the given format: ')
    text = data.split(',')

    age = int(text[4][6:10]) - int(text[3])  # check age
    if age < 12:
        print('   ERROR!: Costumer must be over 12 to register')
        main()

    sql = "insert into customers (F_name, L_name, National_ID, Birth_year, Register_date, Gender)" \
          " values ('" + text[0] + "', '" + text[1] + "', '" + text[2] + "', '" + text[3] + "', '" + text[4] + "', '" + \
          text[5] + "' )"

    cur.execute(sql)

    con.commit()

    print(" insert done! ")

    cur.close()
    tables()
    con.close()


def insert_trip():
    trip_conf = '-2'
    con = database_info
    cur = con.cursor()

    print('\n ------ TRIP REQUEST ------ \n')

    duration = '0'
    destination = "0"
    alt_origin = '0'
    alt_des = '0'
    confirmation_des2 = '0'
    confirmation_org2 = '0'

    # customer
    customer = input('enter customers national ID: ')
    sql = "select national_ID from customers  where national_ID = '" + str(customer) + "' "
    cur.execute(sql)
    data = cur.fetchall()
    if len(data) == 0:
        print('ERROR!: customer does not exist!')
        insert_trip()
    # date
    date_tp = input("enter the trip date (MM/DD/YYYY): ")
    #  origin

    print("----%%%%%%%% ORIGIN  %%%%%%%%----")
    print('\nlist of stations:\n')

    sql = "select ID, station_name from stations ORDER BY id ASC"
    cur.execute(sql)
    data = cur.fetchall()

    for i in data:
        print(i[0], '.', i[1])

    origin = input('choose your origin from the list: ')
    sql = "select available_bicycle, street_name from stations where ID = '" + origin + "' ORDER BY id ASC "

    cur.execute(sql)
    data = cur.fetchall()
    available = data[0][0]
    street_name = data[0][1]

    if available > 0:
        available -= 1
        available = str(available)
        print('good')
        update_origin = "update stations set available_bicycle = '" + available + "' where ID = '" + origin + "' "

        # insert and update

    else:  # alt origin

        print(
            "\nyour chosen station doesn't have any bicycles available right now please choose from the list below:\n ")
        sql = "select ID, station_name  from stations where street_name = '" + street_name + "' AND available_bicycle > 0  "
        cur.execute(sql)
        data = cur.fetchall()

        if len(data) == 0:
            trip_conf = '-1'

            data = [date_tp, origin, destination, customer, alt_des, alt_origin, confirmation_des2, confirmation_org2,
                    duration, trip_conf]

            sql = "insert into trips (date_tp,origin,destination,customer,alt_destination,alt_origin,confirmation_des2,confirmation_org2,duration,trip_conf)" \
                  "values ('" + data[0] + "','" + data[1] + "','" + data[2] + "','" + data[3] + "','" + data[
                      4] + "','" + data[5] + "','" + data[6] + "' ,'" + data[7] + "','" + data[8] + "','" + data[
                      9] + "')"

            cur.execute(sql)
            con.commit()
            print('   no stations available!    \n    TRIP CANCELED BY SYSTEM    ')
            tables()

        else:
            for i in data:
                print(i[0], '.', i[1])

            alt_origin = input("please chose your second origin or press 'Q' to cancel the trip ")
            if alt_origin == 'q' or alt_origin == 'Q':
                confirmation_org2 = '0'
            else:
                print('second origin saved!')
                confirmation_org2 = '1'
                # insert and update

            sql = "select available_bicycle from stations where ID = '" + alt_origin + "' "
            cur.execute(sql)
            data = cur.fetchall()
            available = data[0][0] - 1
            available = str(available)

            update_origin = "update stations set available_bicycle = '" + available + "' where ID = '" + alt_origin + "' "

        # destination  #################################################################
    print("----%%%%%%%% DESTINATION %%%%%%%%----")
    print('list of stations\n:')

    sql = "select ID, station_name from stations ORDER BY id ASC"
    cur.execute(sql)
    data = cur.fetchall()

    for i in data:
        print(i[0], '.', i[1])

    destination = input('choose your destination from the list: ')
    sql = "select available_bicycle, street_name, capacity from stations where ID = '" + destination + "' "

    cur.execute(sql)
    data = cur.fetchall()
    available = data[0][0]
    street_name = data[0][1]
    capacity = data[0][2]
    parking = capacity - available

    if parking > 0:
        available += 1
        available = str(available)
        update_des = "update stations set available_bicycle = '" + available + "' where ID = '" + destination + "' "

        print('good')
        # insert and update

    else:  # alt destination

        print(
            "\nyour chosen station doesn't have any parking space right now. \nplease choose from the list below:\n ")
        sql = "select ID, station_name, capacity from stations where street_name = '" + street_name + "' AND capacity-available_bicycle > 0  "
        cur.execute(sql)
        data = cur.fetchall()
        if len(data) == 0:

            trip_conf = '-1'

            data = [date_tp, origin, destination, customer, alt_des, alt_origin, confirmation_des2, confirmation_org2,
                    duration, trip_conf]

            sql = "insert into trips (date_tp,origin,destination,customer,alt_destination,alt_origin,confirmation_des2,confirmation_org2,duration,trip_conf)" \
                  "values ('" + data[0] + "','" + data[1] + "','" + data[2] + "','" + data[3] + "','" + data[
                      4] + "','" + data[5] + "','" + data[6] + "' ,'" + data[7] + "','" + data[8] + "','" + data[
                      9] + "')"

            cur.execute(sql)
            con.commit()
            print(' no stations available!    \n    TRIP CANCELED BY SYSTEM    \n')
            tables()
        else:
            for i in data:
                print(i[0], '.', i[1])

            alt_des = input("please choose your second destination or press 'Q' to cancel the trip ")

            print('good 2')
            confirmation_des2 = '1'
            # insert and update
            sql = "select available_bicycle from stations where id = '" + alt_des + "' "
            cur.execute(sql)
            data = cur.fetchall()
            available = data[0][0] + 1
            available = str(available)
            update_des = "update stations set available_bicycle = '" + available + "' where ID = '" + alt_des + "' "

    duration = input("enter the trip duration (minutes): ")

    if trip_conf == '-2':
        trip_conf = input('CONFIRM THE TRIP? (0>>NO / 1>>YES)')  # 0>> customer cancel 1>>ok  -1>>cancel by system

        if trip_conf == '1':
            cur.execute(update_origin)
            cur.execute(update_des)

    data = [date_tp, origin, destination, customer, alt_des, alt_origin, confirmation_des2, confirmation_org2, duration,
            trip_conf]

    sql = "insert into trips (date_tp,origin,destination,customer,alt_destination,alt_origin,confirmation_des2,confirmation_org2,duration,trip_conf)" \
          "values ('" + data[0] + "','" + data[1] + "','" + data[2] + "','" + data[3] + "','" + data[4] + "','" + data[
              5] + "','" + data[6] + "' ,'" + data[7] + "','" + data[8] + "','" + data[9] + "')"

    cur.execute(sql)
    con.commit()
    print(" insert done! ")
    tables()


def tables():
    con = database_info
    cursor = con.cursor()

    cursor.execute("select relname from pg_class where relkind='r' and relname !~ '^(pg_|sql_)';")
    table_names = cursor.fetchall()

    print('\n', '*' * 15, 'Tables menu', '*' * 15)

    for i in range(0, len(table_names)):
        print(i + 1, ".", table_names[i][0])
    print('0. MENU')

    choice = int(input('\nchoose a table to insert data: '))

    if choice == 1:
        insert_trip()
    if choice == 2:
        insert_prices()
    if choice == 3:
        insert_stations()
    if choice == 4:
        insert_customers()
    if choice == 0:
        main()


def reports_menu():

    print('\n----$$$  REPORTS MENU  $$$----\n')
    print("1.report1\n2.report2.\n3.report3\n4.report4\n5.report5\n6.report6 \n 0.BACK TO MAIN MENU")
    choice = input('PLEASE CHOSE THE REPORT NUMBER: ')
    if choice == "0":
        main()

    start = input("ENTER THE START DATE: ")
    finish = input("ENTER THE FINISH DATE: ")
    if choice == '1':
        report1(start,finish)
    if choice == "2":
        report2(start, finish)
    if choice == "3":
        report3(start, finish)
    if choice == "4":
        report4(start, finish)
    if choice == "5":
        report5(start, finish)
    if choice == "6":
        report6(start, finish)


def report1(start, finish):

    con = database_info
    cur = con.cursor()

    sql = 'select station_name from stations'
    cur.execute(sql)
    names= cur.fetchall()



    sql = "select station_name, count(origin) from stations right join trips on stations.id = origin where date_tp between '"+start+"' and '"+finish+"' and trip_conf = '1' group by station_name  "
    cur.execute(sql)
    data1 = cur.fetchall()


    sql = "select station_name, count(destination) from trips left join stations on destination = stations.id where date_tp between '"+start+"' and '"+finish+"' and trip_conf = '1' group by station_name  "
    cur.execute(sql)
    data2 = cur.fetchall()

    sql = "select station_name, count(alt_origin) from trips left join stations on alt_origin = stations.id where date_tp between '"+start+"' and '"+finish+"' and trip_conf = '1' group by station_name  "
    cur.execute(sql)
    data3 = cur.fetchall()

    sql = "select station_name, count(alt_destination) from trips left join stations on alt_destination = stations.id where date_tp between '"+start+"' and '"+finish+"' and trip_conf = '1' group by station_name  "
    cur.execute(sql)
    data4 = cur.fetchall()

    # canceled by system
    sql = "select station_name, count(origin) from trips left join stations on origin = stations.id where date_tp between '"+start+"' and '"+finish+"' and trip_conf = '-1' group by station_name  "
    cur.execute(sql)
    bar6 = cur.fetchall()
    # cancel by customer
    sql = "select station_name, count(origin) from trips left join stations on origin = stations.id where date_tp between '" + start + "' and '" + finish + "' and trip_conf = '0' group by station_name  "
    cur.execute(sql)
    bar5 = cur.fetchall()
    # 444444
    sql = "select station_name, count(origin) from trips left join stations on origin = stations.id where date_tp between '" + start + "' and '" + finish + "' and confirmation_des2 = '1' and confirmation_org2 = '1' group by station_name  "
    cur.execute(sql)
    bar4 = cur.fetchall()

    #### 2222222222
    sql = "select station_name, count(origin) from trips left join stations on origin = stations.id where date_tp between '" + start + "' and '" + finish + "'  and confirmation_org2 = '1' group by station_name  "
    cur.execute(sql)
    bar2 = cur.fetchall()

    ##### 3333333
    sql = "select station_name, count(origin) from trips left join stations on origin = stations.id where date_tp between '" + start + "' and '" + finish + "'  and confirmation_des2 = '1' group by station_name  "
    cur.execute(sql)
    bar3 = cur.fetchall()



    total = data1+ data2+data3+data4
    bar1=dict()
    for i in total:
        bar1[i[0]] = bar1.get(i[0],0) + i[1]

    x= list(bar1.keys())
    bar1values= list(bar1.values())



    ###############################
    a = dict()
    b = dict()
    c = dict()
    d = dict()
    e = dict()
    f = dict()


    for i in names:
        a[i[0]] = a.get(i[0],0)
    for i in names:
        b[i[0]] = b.get(i[0],0)
    for i in names:
        c[i[0]] = c.get(i[0],0)
    for i in names:
        d[i[0]] = d.get(i[0],0)
    for i in names:
        e[i[0]] = e.get(i[0],0)
    for i in names:
        f[i[0]] = f.get(i[0], 0)

    for i in names:
        b[i[0]] = b.get(i[0],0)


    for i in bar1.keys():
        a[i] = a.get(i,0) + bar1[i]

    for i in bar2:
        b[i[0]] = b.get(i[0],0) + i[1]

    for i in bar3:
        c[i[0]] = c.get(i[0], 0) + i[1]
    for i in bar4:
        d[i[0]] = d.get(i[0], 0) + i[1]
    for i in bar5:
        e[i[0]] = e.get(i[0], 0) + i[1]
    for i in bar6:
        f[i[0]] = f.get(i[0], 0) + i[1]

    ################################

    a.pop(None)
    x.pop(-1)

    x = list(bar1.keys())
    a1 = np.array(list(a.values()))
    b1 = np.array(list(b.values()))
    c1 = np.array(list(c.values()))
    d1 = np.array(list(d.values()))
    e1 = np.array(list(e.values()))
    f1 = np.array(list(f.values()))

    plt.bar(list(b.keys()), a1, label='total')
    plt.bar(list(b.keys()), b1,bottom=a1,label='2')
    plt.bar(list(b.keys()), c1,bottom=(b1+a1),label='3')
    plt.bar(list(b.keys()), d1,bottom=b1+a1+c1,label='4')
    plt.bar(list(b.keys()), e1,bottom=b1+a1+c1+d1,label='5')
    plt.bar(list(b.keys()), f1,bottom=b1+a1+c1+d1+e1,label='6')
    plt.title('REPORT 1')
    plt.xlabel('stations')
    plt.ylabel('demand')
    plt.legend()
    plt.show()



def report2(start, finish):

    con = database_info
    cur = con.cursor()

    sql = " select origin,count(trip_conf) from trips where trip_conf = '1' and confirmation_org2 = 'false'  AND date_tp between '"+start+"' and '"+finish+"' group by origin "
    sql2 = " select destination,count(trip_conf) from trips where trip_conf = '1' and confirmation_des2 = 'false' AND date_tp between '"+start+"' and '"+finish+"' group by destination "
    sql3 = " select alt_origin,count(trip_conf) from trips where trip_conf = '1' AND date_tp between '"+start+"' and '"+finish+"' group by alt_origin "
    sql4 = " select alt_destination,count(trip_conf) from trips where trip_conf = '1' AND date_tp between '"+start+"' and '"+finish+"' group by alt_destination "
    cur.execute(sql)
    data=cur.fetchall()

    cur.execute(sql2)
    data2 = cur.fetchall()

    cur.execute(sql3)
    data3 = cur.fetchall()

    cur.execute(sql4)
    data4 = cur.fetchall()

    total = data4+data3+data2+data

    b= []
    v= []


    a=dict()
    for i in total:
        a[i[0]] = a.get(i[0],0) + i[1]

    print("    BEST PERFORMANCE")
    k = list(a.keys())
    l = list(a.values())
    for i in range(0,len(k)):
        if k[i] !=0:

            b.append(k[i])
            b.append(l[i])
            v.append(b)
            b=[]


    v.sort(key=lambda x:x[1],reverse=True)
    print(tabulate(v[0:3], headers=['Station ID', ' Demand']))


    sql = " select origin,count(trip_conf) from trips where trip_conf = '0' or trip_conf = '-1' and confirmation_org2 = 'false' group by origin "
    sql2 = " select destination,count(trip_conf) from trips where trip_conf = '0' or trip_conf = '-1' and confirmation_des2 = 'false' group by destination "
    sql3 = " select alt_origin,count(trip_conf) from trips where trip_conf = '0' or trip_conf = '-1' group by alt_origin "
    sql4 = " select alt_destination,count(trip_conf) from trips where trip_conf = '0' or trip_conf = '-1' group by alt_destination "
    cur.execute(sql)
    data = cur.fetchall()

    cur.execute(sql2)
    data2 = cur.fetchall()

    cur.execute(sql3)
    data3 = cur.fetchall()

    cur.execute(sql4)
    data4 = cur.fetchall()
    total = data4+data3+data2+data

    b= []
    v= []

    a=dict()
    for i in total:
        a[i[0]] = a.get(i[0],0) + i[1]

    print('\n',"    WORST PERFORMANCE")
    k = list(a.keys())
    l = list(a.values())
    for i in range(0,len(k)):
        if k[i] !=0:

            b.append(k[i])
            b.append(l[i])
            v.append(b)
            b=[]


    v.sort(key=lambda x:x[1],reverse=True)
    print(tabulate(v[0:3], headers=['Station ID', 'Canceled Demand']))
    reports_menu()


def report3(start, finish):
    con = database_info
    cur = con.cursor()
    sql = " select national_ID, L_name from customers "
    cur.execute(sql)
    customers = cur.fetchall()
    header = ['ID', 'Customers']
    print(tabulate(customers, headers= header) )
    c_id = input('enter customer ID: ')

    sql = "select station_name , duration from trips inner join stations on origin = stations.id  where customer = '"+c_id+"' and trip_conf = '1' and date_tp between '"+start+"' and '"+finish+"' "
    sql2 = "select station_name from trips inner join stations on destination = stations.id  where customer = '" + c_id + "' and trip_conf = '1' and date_tp between '"+start+"' and '"+finish+"'"
    cur.execute(sql)

    data = cur.fetchall()
    cur.execute(sql2)
    data2 = cur.fetchall()


    org = []
    des = []
    size = []

    for i in data:
        org.append(i[0])
        size.append(i[1])
    print(org)

    for i in data2:
        des.append(i[0])
    print(des)

    plt.scatter(org, des, s=size)
    plt.title("customer '"+c_id+"' performance ")
    plt.show()
    reports_menu()


def report4(start, finish):


    con = database_info
    cur = con.cursor()

    sql = "select date_tp,sum(duration * price_per_minute) from trips inner join prices on extract(year from date_tp) = extract(year from price_year) where trip_conf = '1' and date_tp between '"+start+"' and '"+finish+"' group by date_tp  "
    sql2 = "select date_tp, sum(-cancelation_fee) from trips inner join prices on extract(year from date_tp) = extract(year from price_year) where trip_conf = '0' or trip_conf = '-1' and date_tp between '"+start+"' and '"+finish+"' group by date_tp  "

    cur.execute(sql)
    trips = cur.fetchall()

    cur.execute(sql2)
    trips2 = cur.fetchall()

    trips.sort()
    trips2.sort()



    x=[]
    y=[]
    for i in trips:
        x.append(i[0])
        y.append(i[1])

    for i in trips2:
        x.append(i[0])
        y.append(i[1])

    print(x)

    plt.bar(x, y)
    plt.show()
    reports_menu()


def report5(start, finish):

    con = database_info
    cur = con.cursor()

    sql = "select count(gender)  from customers where register_date between '"+start+"' and '"+finish+"' group by gender"
    cur.execute(sql)
    data = cur.fetchall()
    datalist = []
    for i in data:
        datalist.append(i[0])
    # plot
    lang = ['Male', 'Female']
    plt.pie(datalist, labels=lang, autopct='%1.1f%%')
    plt.title('Customers registered based on gender')
    plt.show()
    reports_menu()


def report6(start, finish):

    con = database_info
    cur = con.cursor()

    age_sql = "select f_name, L_name, national_id from customers order by birth_year "
    cur.execute(age_sql)
    data = cur.fetchall()

    oldest_id = str(data[0][2])
    youngest_id = str(data[-1][2])




    oldest_sql = "select * from trips where date_tp between '"+start+"' and '"+finish+"' AND customer = '"+oldest_id+"' "
    cur.execute(oldest_sql)
    data_old = cur.fetchall()
    header = ['ID', 'Customer', 'Origin', 'Destination','second DES', 'Second ORG', 'Date','Confirm second Des','Confirm second ORG', 'duration', 'Confirm' ]
    print('oldest customer: ', data[0][0] + ' ' + data[0][1], 'ID: ', oldest_id)
    print(tabulate(data_old, headers=header))

    print('\n\nyoungest customer: ', data[-1][0] + ' ' + data[-1][1], "ID: ", youngest_id)
    young_sql = "select * from trips where date_tp between '"+start+"' and '"+finish+"' AND customer = '"+youngest_id+"' "
    cur.execute(young_sql)
    data_young = cur.fetchall()
    print(tabulate(data_young, headers=header))

    reports_menu()




def main():
    print('\n', '*' * 15, 'MENU', '*' * 15)
    print('\n 1.create database \n 2.create tables \n 3.insert data \n 4.reports')
    choice = int(input('\n enter the number from the menu: '))

    if choice == 1:
        create_database()
    if choice == 2:
        create_table()
    if choice == 3:
        tables()
    if choice == 4:
        reports_menu()


main()
