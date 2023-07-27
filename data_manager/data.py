import sqlalchemy
from sqlalchemy import create_engine

QUERY_FLIGHT_BY_ID = "SELECT airlines.airline AS AIRLINE, flights.ID, flights.ORIGIN_AIRPORT, " \
                     "flights.DESTINATION_AIRPORT, flights.DEPARTURE_DELAY as DELAY FROM flights " \
                     "JOIN airlines ON flights.airline = airlines.id WHERE flights.ID = :id"
QUERY_FLIGHTS_BY_DATE = "SELECT flights.AIRLINE, flights.ID, flights.ORIGIN_AIRPORT, " \
                        "flights.DESTINATION_AIRPORT,flights.DEPARTURE_DELAY AS DELAY FROM " \
                        "flights WHERE DAY LIKE :day AND MONTH LIKE :month AND YEAR LIKE :year"
QUERY_DELAYED_FLIGHTS_BY_AIRLINE = "SELECT airlines.airline AS AIRLINE, airlines.ID AS ID, " \
                                   "flights.ORIGIN_AIRPORT, flights.DESTINATION_AIRPORT, " \
                                   "flights.DEPARTURE_DELAY AS DELAY FROM flights JOIN airlines ON " \
                                   "flights.AIRLINE = airlines.ID WHERE airlines.AIRLINE LIKE " \
                                   ":airline_name AND flights.DEPARTURE_DELAY > 0 " \
                                   "AND flights.DEPARTURE_DELAY IS NOT ''"
QUERY_DELAYED_FLIGHTS_BY_AIRPORT = "SELECT airlines.AIRLINE, flights.ID, flights.ORIGIN_AIRPORT," \
                                   " flights.DESTINATION_AIRPORT, flights.DEPARTURE_DELAY AS DELAY " \
                                   "FROM flights JOIN airlines ON flights.AIRLINE = airlines.ID WHERE" \
                                   " flights.ORIGIN_AIRPORT LIKE :iata_code AND flights.DEPARTURE_DELAY" \
                                   " > 0 AND flights.DEPARTURE_DELAY IS NOT ''"


class FlightData:
    """
    The FlightData class is a Data Access Layer (DAL) object that provides an
    interface to the flight data in the SQLITE database. When the object is created,
    the class forms connection to the sqlite database file, which remains active
    until the object is destroyed.
    """

    def __init__(self, db_uri):
        """
        Initialize a new engine using the given database URI
        """
        self._engine = create_engine(db_uri)

    def _execute_query(self, query, params={}):
        """
        Execute an SQL query with the params provided in a dictionary,
        and returns a list of records (dictionary-like objects).
        If an exception was raised, print the error, and return an empty list.
        :param params:
        :param query:
        :return list:
        """
        results_list = []

        with self._engine.connect() as connection:
            _query = sqlalchemy.text(query)
            results = connection.execute(_query, params)
            rows = results.fetchall()

        for row in rows:
            temp_result_dictionary = {'DELAY': row[4],
                                      'ORIGIN_AIRPORT': row[2],
                                      'DESTINATION_AIRPORT': row[3],
                                      'ID': row[1],
                                      'AIRLINE': row[0]
                                      }
            results_list.append(temp_result_dictionary)
        return results_list

    def get_flight_by_id(self, flight_id):
        """
        Searches for flight details using flight ID.
        If the flight was found, returns a list with a single record.
        :param flight_id:
        :return list:
        """

        params = {'id': flight_id}
        return self._execute_query(QUERY_FLIGHT_BY_ID, params)

    def get_flights_by_date(self, day, month, year):
        """
            Searches for flight details using date and returns all records matching
            the given date
        :param day:
        :param month:
        :param year:
        :return list:
        """

        params = {'day': day, 'month': month, 'year': year}
        return self._execute_query(QUERY_FLIGHTS_BY_DATE, params)

    def get_delayed_flights_by_airline(self, airline_name):
        """
            Searches for flight details using airline name that flights got delayed and
            returns matching records found
        :param airline_name:
        :return list:
        """

        params = {'airline_name': airline_name}
        return self._execute_query(QUERY_DELAYED_FLIGHTS_BY_AIRLINE, params)

    def get_delayed_flights_by_airport(self, iata_code):
        """
            Searches for flight details using airport code that flights got delayed and
            returns matching records found
        :param iata_code:
        :return:
        """

        params = {'iata_code': iata_code}
        return self._execute_query(QUERY_DELAYED_FLIGHTS_BY_AIRPORT, params)

    def __del__(self):
        """
        Closes the connection to the database when the object is about to be destroyed
        """
        self._engine.dispose()
