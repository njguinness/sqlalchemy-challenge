# Import the dependencies.

import numpy as np
import datetime as dt

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, and_

from flask import Flask, jsonify

engine = create_engine("sqlite:///../Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()

# reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to the table
measurement = Base.classes.measurement
station = Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route('/')
def welcome():
    """List all available API Routes"""
    return(
        f'Available Routes:<br/>'
        f'/api/v1.0/precipitation<br/>'
        f'/api/v1.0/stations<br/>'
        f'/api/v1.0/tobs<br/>'
        f'/api/v1.0/<start><br/>'
        f'/api/v1.0/<start>/<end>'
    )

@app.route('/api/v1.0/precipitation')
def precipitation():
    #Open Python session to database
    session = Session(engine)

    #Query for 12 months of precipitation
    recent_date = session.query(measurement.date).order_by(measurement.date.desc()).first()
    last_year_date = (dt.datetime.strptime(recent_date[0],'%Y-%m-%d') - dt.timedelta(days=365)).strftime('%Y-%m-%d')
    results = session.query(measurement.date, measurement.prcp).filter(measurement.date >= last_year_date).all()
    #Create dictionary and add values from precipitation_data query
    precipitation_list = []

    for row in results:
        date_dict = {}
        date_dict[row.date] = row.prcp
        precipitation_list.append(date_dict)
    
    #Return to api
    return jsonify(precipitation_list)

@app.route('/api/v1.0/stations')
def stations():
    #Open Python session to database
    session = Session(engine)

    #Generate and return a list of stations
    results = session.query(station.station).all()

    #Convert to normal list
    station_list = list(np.ravel(results))
    
    return jsonify(station_list)

@app.route('/api/v1.0/tobs')
def tobs():
    #Open Python session to database
    session = Session(engine)
    
    #Save calculated varibles
    recent_date = session.query(measurement.date).order_by(measurement.date.desc()).first()
    last_year_date = (dt.datetime.strptime(recent_date[0],'%Y-%m-%d') - dt.timedelta(days=365)).strftime('%Y-%m-%d')

    #Query the dates and temperature observations of the most-active station for the previous year of data.
    results = session.query(measurement.date, measurement.tobs).filter(measurement.station == 'USC00519281').\
        filter(measurement.date >= last_year_date).all()
    
    #Convert to normal list
    most_active_data = list(np.ravel(results))
    
    return jsonify(most_active_data)

@app.route("/api/v1.0/<start>")
def temp_range_start(start):
    #Open Python session to database
    session = Session(engine)
    #Query to find the min, max, average for specified date
    results = session.query(measurement.date, func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs).\
        filter(measurement.date >= start).group_by(measurement.date).all())

    session.close()

    # Create a dictionary from the row data and append to a list of temps
    temp_list = []
    for min, max, avg, date in results:
        temp_dict = {}
        temp_dict['TMIN'] = min
        temp_dict['TMAX'] = max
        temp_dict['TAVG'] = avg
        temp_dict['Date'] = date
        temp_list.append(temp_dict)

    return jsonify(temp_list)

@app.route("/api/v1.0/<start>/<end>")
def temp_range_start_end(start, end):
    #Open Python session to database
    session = Session(engine)
    #Query to find the min, max, average for specified date ranges
    results = session.query(measurement.date, func.min(measurement.tobs), func.avg(measurement.tobs), func.max(measurement.tobs).\
        filter(measurement.date >= start, measurement.date <= end).group_by(measurement.date).all())
    
    session.close()
    
    # Create a dictionary from the row data and append to a list of temps
    temp_list = []
    for min, max, avg, date in results:
        temp_dict = {}
        temp_dict['TMIN'] = min
        temp_dict['TMAX'] = max
        temp_dict['TAVG'] = avg
        temp_dict['Date'] = date
        temp_list.append(temp_dict)

    return jsonify(temp_list)

#Call Flask to run
if __name__ == '__main__':
    app.run(debug=True)
