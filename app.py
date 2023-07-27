from flask import Flask, jsonify, request
from data_manager import data
from datetime import datetime


SQLITE_URI = 'sqlite:///storage/flights.sqlite3'

data_manager = data.FlightData(SQLITE_URI)

app = Flask(__name__)


@app.route('/api/gfb_id/<int:flight_id>')
def get_flight_by_id(flight_id):
    """
        Gets triggered when a client request is sent to its endpoint.
        Queries the database and returns a record of flight that matches the provided flight ID
    :param flight_id:
    :return:
    """
    try:
        results = data_manager.get_flight_by_id(flight_id)
        if not results:
            return jsonify({"message": "Oops, there is no data matching your query, try another flight id"})
        else:
            return jsonify(results)
    except Exception:
        return jsonify({"error": "Invalid Request"}), 400


@app.route('/api/gfb_date')
def get_flight_by_date():
    """
        Gets triggered when a client request is sent to its endpoint.
        Queries the database and returns records of flights on the given date
    :return:
    """
    try:
        date_input = request.args.get('date')
        date = datetime.strptime(date_input, '%d/%m/%Y')
        results = data_manager.get_flights_by_date(date.day, date.month, date.year)
        if not results:
            return jsonify({"message": "Oops, there is no data matching your query, try another date"})
        else:
            return jsonify(results)
    except Exception:
        return jsonify({"error": "Invalid Request"}), 400


@app.route('/api/gdfb_airline')
def get_delayed_flights_by_airline():
    """
        Gets triggered when a client request is sent to its endpoint.
        Queries the database and returns records of the queried airline whose flights are delayed
    :return:
    """
    try:
        airline_input = request.args.get('airline')
        results = data_manager.get_delayed_flights_by_airline(airline_input)
        if not results:
            return jsonify({"message": "Oops, there is no data matching your query, try another airline"})
        else:
            return jsonify(results)
    except Exception:
        return jsonify({"error": "Invalid Request"}), 400


@app.route('/api/gdfb_airport')
def get_delayed_flights_by_airport():
    """
        Gets triggered when a client request is sent to its endpoint.
        Queries the database and returns records of the queried airport whose flights are delayed
    :return:
    """
    try:
        airport_input = request.args.get('airport')
        results = data_manager.get_delayed_flights_by_airport(airport_input)
        if not results:
            return jsonify({"message": "Oops, there is no data matching your query, try another airport"})
        else:
            return jsonify(results)
    except Exception:
        return jsonify({"error": "Invalid Request"}), 400


@app.errorhandler(404)
def page_not_found(error):
    """
        Handles 404 error when it is triggered by client
    :param error:
    :return:
    """
    return jsonify({"error": "Not Found"}), 404


@app.errorhandler(400)
def page_not_found(error):
    """
        Handles 400 error when it is triggered by client
    :param error:
    :return:
    """
    return jsonify({"error": "Not Found"}), 400


@app.errorhandler(500)
def internal_server_error(error):
    """
        Handles 500 error when it is triggered by client
    :param error:
    :return:
    """
    return jsonify({"error": "Not Found"}), 500


@app.errorhandler(405)
def internal_server_error(error):
    """
        Handles 405 error when triggered by client
    :param error:
    :return:
    """
    return jsonify({"error": "Not Found"}), 405


if __name__ == "__main__":
    # Launch the Flask dev server
    app.run(host="0.0.0.0", port=5000, debug=True)
