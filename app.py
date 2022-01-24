import csv

import requests
import json
import sqlite3
import flask
from flask import request, jsonify
import re

app = flask.Flask(__name__)

# api = Api(app)

urls = "https://www.metaweather.com/api/location/"
conn = sqlite3.connect('final_db.db', check_same_thread=False)
cursor = conn.cursor()
date = ['2021/12/6', '2021/12/7', '2021/12/8', '2021/12/9', '2021/12/10', '2021/12/11', '2021/12/12']
date_id = [re.sub(r'/', '-', file) for file in date]


def get_cities(cities):
    t = []
    for city in cities:
        res = requests.get(urls + 'search/?query=' + city)
        t.append(res.json())
    # print(t)
    id_list = []
    j = 0
    while j < 3:
        for item in t[j]:
            id_list.append(item['woeid'])
            j += 1
    return id_list


def get_weather(id):
    e = []
    # init_date = str(datetime.date(datetime.now()))
    # print(init_date)

    for idx in range(3):
        temp = []
        for days in date:
            res = requests.get(urls + str(id[idx]) + "/" + days + "/")
            temp.append(res.json())
        e.append(temp)
    return e


def query1():
    # FIRST
    json_data1 = []
    cursor.execute(
        "SELECT * FROM Forecasts where hour_2 in (select max(hour_2) from Forecasts group by Days_idDays, Cities_idCities)")
    # this will extract row headers
    row_headers = [x[0] for x in cursor.description]
    rv = cursor.fetchall()

    # for csv
    csvWriter = csv.writer(open("query1.csv", "w"))
    csvWriter.writerows(rv)

    for result in rv:
        json_data1.append(dict(zip(row_headers, result)))
    return json_data1


def query2():
    # SECOND
    json_data2 = []
    cursor.execute(
        "create table if not exists query_2 as select * from (select temp, Cities_idCities, Days_idDays,hour_2,"
        "row_number() over (partition by Days_idDays, Cities_idCities order by hour_2 desc) as hour_rank from "
        "Forecasts) ranks where hour_rank <= 3")
    cursor.execute(
        "SELECT Days_idDays, Cities_idCities, avg(temp) FROM Forecasts GROUP BY Cities_idCities, Days_idDays;")
    row_headers = [x[0] for x in cursor.description]
    rv = cursor.fetchall()

    # for csv

    csvWriter2 = csv.writer(open("query2.csv", "w"))
    csvWriter2.writerows(rv)

    for result in rv:
        json_data2.append(dict(zip(row_headers, result)))
    return json_data2


def api_app():

    json_data1 = query1()
    json_data2 = query2()

    @app.route('/')
    def index():
        return "Welcome"

    @app.route("/forecasts", methods=['GET'])
    def forecasts():
        return jsonify({'Forecasts': json_data1})

    @app.route("/average", methods=['GET'])
    def average():
        return jsonify({'Average temperatures': json_data2})

    @app.route("/locations/<n>")
    def top_n(n):
        json_data3 = []
        cursor.execute(
            "select * from Forecasts, Cities where Forecasts.Cities_idCities=Cities.idCities order by temp, max_temp, "
            "min_temp, wind_speed, wind_direction, air_pressure, humidity, predictability  desc limit ?",
            (n,))
        row_headers_n = [x[0] for x in cursor.description]
        rv_ = cursor.fetchall()

        for result in rv_:
            json_data3.append(dict(zip(row_headers_n, result)))

        # for csv

        csvWriter3 = csv.writer(open("query3.csv", "w"))
        csvWriter3.writerows(rv_)

        return jsonify({'Cities with top temperatures': json_data3})


def insert_to_db(cities, id_list, city_db):

    for i in range(len(cities)):
        cursor.executemany("INSERT INTO Cities (idCities, name) VALUES (?, ?)", [(int(id_list[i]), cities[i])])
    for i in range(len(date)):
        cursor.executemany("INSERT INTO Days (idDays, dates) VALUES (?, ?)", [(i, date_id[i])])

    for i in range(len(city_db)):
        cursor.executemany(
            "INSERT INTO Forecasts (idForecasts, Days_idDays, Cities_idCities, hour_2, temp, state, wind, max_temp, "
            "min_temp, wind_speed, wind_direction, air_pressure, humidity, predictability) VALUES (?, ?, ?, ?, ?, ?, "
            "?, ?, ?, ?, ?, ?, ?, ?)",
            city_db[i])

    conn.commit()


def main():
    cities = ["Berlin", "Rome", "Athens"]
    # for query in range(3):
    #    cities.append(input("Give city: "))

    id_list = get_cities(cities)
    print("id list: ", id_list)
    weather = get_weather(id_list)
    # city_db contains the information that will be passed in Forecasts tables. It is a list of three cities,
    # each one of them containing a list of seven dates, each one of them contains a list with dictionaries
    # (different forecasts about one date)
    city_db = []
    for city in range(len(weather)):
        for day in range(len(weather[city])):
            for forecast in range(len(weather[city][day])):
                city_db.append([(weather[city][day][forecast]["id"],
                                      weather[city][day][forecast]["applicable_date"], id_list[city],
                                      weather[city][day][forecast]["created"], weather[city][day][forecast]["the_temp"],
                                      weather[city][day][forecast]["weather_state_name"],
                                      weather[city][day][forecast]["wind_direction_compass"],
                                      weather[city][day][forecast]["max_temp"],
                                      weather[city][day][forecast]["min_temp"],
                                      weather[city][day][forecast]["wind_speed"],
                                      weather[city][day][forecast]["wind_direction"],
                                      weather[city][day][forecast]["air_pressure"],
                                      weather[city][day][forecast]["humidity"],
                                      weather[city][day][forecast]["predictability"])])
    #print(city_db)

    api_app()

    # -------------------------------- INSERT INTO DATABASE -----------------------------------------------
    # uncomment the next line to insert
    #insert_to_db(cities, id_list, city_db)


if __name__ == '__main__':
    main()
    app.run(debug=True)
