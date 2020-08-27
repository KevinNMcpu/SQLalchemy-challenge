#import dependencies
import sqlalchemy
import pandas as pd
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify
from datetime import date
from datetime import datetime

#DB setup
engine = create_engine("sqlite:///Resources/hawaii.sqlite",connect_args={'check_same_thread': False})#check_same_thread false fixes a bug where the Flask server would crash if you went to a second page without restarting the Flask server
session = Session(engine)
Base = automap_base()
Base.prepare(engine, reflect=True)
Measurement = Base.classes.measurement
Station = Base.classes.station

#create an app
app = Flask(__name__)

#define index route
@app.route("/")
def home():
    print("Server received request for Home page...")
    return (
        f"Welcome to my home page!<br/><br/>"
        f"localhost:5000/api/v1.0/precipitation will return total precipitation by date!<br/>"
        f"localhost:5000/api/v1.0/stations will a list of all weather stations!<br/>"
        f"localhost:5000/api/v1.0/tobs will temperature data for the last year from the most active weather station (USC00519281)!<br/>"
        f"localhost:5000/api/v1.0/YYYYMMDD will return temperature data from that year, month and day to the last data we have!<br/>"
        f"localhost:5000/api/v1.0/YYYYMMDD/YYYYMMDD will return temperature data from that date range<br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    print("Retrieving Total Precipitation by Date")
    results = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= "2016-08-23")
    date_data = [result[0] for result in results[:]]
    prcp_data = [result[1] for result in results[:]]
    prcp_data_df = pd.DataFrame(list(zip(date_data, prcp_data)),columns=["Date","Precip"])
    prcp_data_df = prcp_data_df.groupby(["Date"]).sum()
    prcp_data_df = prcp_data_df.reset_index()
    prcp_data_dict = prcp_data_df.to_dict("Date")
    return jsonify(prcp_data_dict)

@app.route("/api/v1.0/stations")
def stations():
    print("Retrieving list of stations")
    results2 = session.query(Measurement.station)
    station_data = [result[0] for result in results2[:]]
    no_duplicate_station_data = list(set(station_data))
    return jsonify(no_duplicate_station_data)

@app.route("/api/v1.0/tobs")
def tobs():
    print("Retrieving list of temperature data for USC00519281")
    #homework instructions say to query date + temp but then to just return a list of temps?
    results3 = session.query(Measurement.date, Measurement.tobs).filter(Measurement.station == "USC00519281").filter(Measurement.date >= "2016-08-23")
    date_data2 = [result[0] for result in results3[:]]
    tobs_data = [result[1] for result in results3[:]]
    return jsonify(tobs_data)

@app.route("/api/v1.0/<start>")
def temp_range1(start):
    print("Retrieving....")
    start_int = int(start)
    if start_int > 20170823:
        return "Please pick dates in between 2010/01/01 and 2017/08/23"
    elif start_int < 20100101:
        return "Please pick dates in between 2010/01/01 and 2017/08/23"
    else:
        start = str(start_int)
        start_date = datetime.strptime(start, '%Y%m%d')
        start_date_string = start_date.strftime('%Y-%m-%d')
        weather_dict = {"Min Temp": session.query(func.min(Measurement.tobs).filter(Measurement.date >= start_date_string)).scalar(),"Avg Temp":session.query(func.avg(Measurement.tobs).filter(Measurement.date >= start_date_string)).scalar(),"Max Temp":session.query(func.max(Measurement.tobs).filter(Measurement.date >= start_date_string)).scalar()}
        return weather_dict

@app.route("/api/v1.0/<start>/<stop>")
def temp_range2(start, stop):
    print("Retrieving....")
    start_int = int(start)
    stop_int = int(stop)
    if start_int > 20170823:
        return "Please pick dates in between 2010/01/01 and 2017/08/23"
    elif start_int < 20100101:
        return "Please pick dates in between 2010/01/01 and 2017/08/23"
    elif stop_int > 20170823:
        return "Please pick dates in between 2010/01/01 and 2017/08/23"
    elif stop_int < 20100101:
        return "Please pick dates in between 2010/01/01 and 2017/08/23"
    else:
        start = str(start_int)
        stop = str(stop_int)
        start_date = datetime.strptime(start, '%Y%m%d')
        start_date_string = start_date.strftime('%Y-%m-%d')
        stop_date = datetime.strptime(stop, '%Y%m%d')
        stop_date_string = stop_date.strftime('%Y-%m-%d')
        weather_dict = {"Min Temp": session.query(func.min(Measurement.tobs).filter(Measurement.date >= start_date_string).filter(Measurement.date <= stop_date_string)).scalar(),"Avg Temp":session.query(func.avg(Measurement.tobs).filter(Measurement.date >= start_date_string).filter(Measurement.date <= stop_date_string)).scalar(),"Max Temp":session.query(func.max(Measurement.tobs).filter(Measurement.date >= start_date_string).filter(Measurement.date <= stop_date_string)).scalar()}
        return weather_dict