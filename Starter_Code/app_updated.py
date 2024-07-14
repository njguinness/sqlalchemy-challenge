import numpy as np
import datetime as dt

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

# Import Flask
from flask import Flask, jsonify, request

#################################################
# Database Setup
#################################################

# Create engine to hawaii.sqlite
engine = create_engine("sqlite:///../Starter_Code/Resources/hawaii.sqlite")

# Reflect the existing database into a new model
Base = automap_base()

# Reflect the tables
Base.prepare(autoload_with=engine)

# Save reference to the tables
Measurement = Base.classes.measurement
Station = Base.classes.station


#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

@app.route("/")
def homepage():
    """List all available API routes."""
    return (
        f"Welcome to SurfsUp API<br/>"
        f"Available Routes:<br/>"
        f"_______________________<br/>"
        f"Precipitation data for last 12 months (2016-08-24 to 2017-08-23):<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"_______________________<br/>"
        f"List of stations in the dataset:<br/>"
        f"/api/v1.0/stations<br/>"
        f"_______________________<br/>"
        f"Date and temperature observations of the most-active station (USC00519281) for previous year:<br/>"
        f"/api/v1.0/tobs<br/>"
        f"_______________________<br/>"
        f"Minimum, Maximum and average temperature from a given start date to end of dataset:<br/>"
        f"Please enter Start Date in the format: YYYY-MM-DD<br/>"
        f"/api/v1.0/<start><br/>"
        f"_______________________<br/>"
        f"Minimum, Maximum and average temperature from a given start date to a given end date:<br/>"
        f"Please enter Start Date and End Date in the format: YYYY-MM-DD/YYYY-MM-DD<br/>"
        f"/api/v1.0/<start>/<end><br/>"
        f"_______________________"
    )


@app.route("/api/v1.0/precipitation")
def precipitation():
    """Retrieve last 12 months of data"""

    # Create our session (link) from Python to the DB
    session = Session(engine)

    try:
        # Find the most recent date in the dataset.
        latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first().date

        # Calculate the date one year from the last date in data set.
        year_ago_date = dt.datetime.strptime(latest_date, '%Y-%m-%d') - dt.timedelta(days=365)

        # Perform a query to retrieve the data and precipitation scores
        prcp_scores = session.query(Measurement.date, Measurement.prcp).\
                        filter(Measurement.date >= year_ago_date).\
                        order_by(Measurement.date).all()
        
        # Create a dictionary using date as the key and prcp as the value
        prcp_dict = {date: prcp for date, prcp in prcp_scores}
        
        return jsonify(prcp_dict)
    
    except Exception as e:
        return jsonify({"error": str(e)})
    
    finally:
        # Close the session
        session.close()


@app.route("/api/v1.0/stations")
def stations():
    """Get a list of stations"""
    
    # Create our session (link) from Python to the DB
    session = Session(engine)

    try:
        # Perform a query to retrieve the station data
        station_data = session.query(Station.station).all()
        
        # Convert list of tuples into normal list
        station_list = list(np.ravel(station_data))
        
        return jsonify(station_list)
    
    except Exception as e:
        return jsonify({"error": str(e)})
    
    finally:
        # Close the session
        session.close()


@app.route("/api/v1.0/tobs")
def tobs():
    """Get temperature data for the most active station for last year"""
    
    # Create our session (link) from Python to the DB
    session = Session(engine)

    try:
        # Find the most recent date in the dataset.
        latest_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first().date

        # Calculate the date one year from the last date in data set.
        year_ago_date = dt.datetime.strptime(latest_date, '%Y-%m-%d') - dt.timedelta(days=365)

        # Find the most active station id
        most_active_station = session.query(Measurement.station).\
                                group_by(Measurement.station).\
                                order_by(func.count(Measurement.station).desc()).first()[0]

        # Perform a query to get the dates and temperature observations of the most-active station
        # for the previous year of data
        station_temp_data = session.query(Measurement.date, Measurement.tobs).\
                                    filter(Measurement.station == most_active_station).\
                                    filter(Measurement.date >= year_ago_date).\
                                    order_by(Measurement.date).all()
        
        # Create a list of dictionary containing dates and temperatures observations
        station_temp_list = [{"date": date, "temp": temp} for date, temp in station_temp_data]
        
        return jsonify(station_temp_list)
    
    except Exception as e:
        return jsonify({"error": str(e)})
    
    finally:
        # Close the session
        session.close()


@app.route("/api/v1.0/<start>")
def start(start):
    """Get min, max and average temperature data from start date"""
    
    # Create our session (link) from Python to the DB
    session = Session(engine)

    try:
        # Create list for date and temperature values
        sel = [Measurement.date,
               func.min(Measurement.tobs), 
               func.max(Measurement.tobs), 
               func.avg(Measurement.tobs)]

        # Perform a query to get TMIN, TAVG, and TMAX for all the dates 
        # greater than or equal to the start date, taken as a parameter from the URL
        start_data = session.query(*sel).\
                        filter(func.strftime("%Y-%m-%d", Measurement.date) >= start).\
                        group_by(Measurement.date).\
                        order_by(Measurement.date).all()
        
        # Create a list of dictionary to store date, min, max and avg temperature values
        start_data_list = [{"date": date, "min_temp": min, "max_temp": max, "avg_temp": avg} for date, min, max, avg in start_data]
        
        return jsonify(start_data_list)
    
    except Exception as e:
        return jsonify({"error": str(e)})
    
    finally:
        # Close the session
        session.close()


@app.route("/api/v1.0/<start>/<end>")
def start_end(start, end):
    """Get min, max and average temperature data from start date to end date"""
    
    # Create our session (link) from Python to the DB
    session = Session(engine)

    try:
        # Create list for date and temperature values
        sel = [Measurement.date,
               func.min(Measurement.tobs), 
               func.max(Measurement.tobs), 
               func.avg(Measurement.tobs)]

        # Perform a query to get TMIN, TAVG, and TMAX for all the dates from start date to
        # end date inclusive, taken as parameters from the URL
        start_end_data = session.query(*sel).\
                        filter(func.strftime("%Y-%m-%d", Measurement.date) >= start).\
                        filter(func.strftime("%Y-%m-%d", Measurement.date) <= end).\
                        group_by(Measurement.date).\
                        order_by(Measurement.date).all()
        
        # Create a list of dictionary to store date, min, max and avg temperature values
        start_end_data_list = [{"date": date, "min_temp": min, "max_temp": max, "avg_temp": avg} for date, min, max, avg in start_end_data]
        
        return jsonify(start_end_data_list)
    
    except Exception as e:
        return jsonify({"error": str(e)})
    
    finally:
        # Close the session
        session.close()


#################################################

if __name__ == '__main__':
    app.run(debug=True)
