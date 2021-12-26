import sqlite3
import csv

class Database:
    def __init__(self, db):
        self.connection = sqlite3.connect(db)
        self.cursor = self.connection.cursor()
        self.cursor.execute("CREATE TABLE IF NOT EXISTS FlightLeg(id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL, tailNumber CHAR(6) NOT NULL, sourceAirportCode CHAR(3) NOT NULL, destinationAirportCode CHAR(3) NOT NULL, sourceCountryCode CHAR(3) NOT NULL, destinationCountryCode CHAR(3) NOT NULL, departureTimeUtc DATETIME NOT NULL, landingTimeUtc DATETIME NOT NULL)")
        self.connection.commit()

    def insert(self, filename):
        with open(filename, encoding='utf-8') as csvFile:
            reader = csv.reader(csvFile, delimiter=';')
            next(reader)
            for row in reader:
                self.cursor.execute("INSERT INTO FlightLeg VALUES (NULL, ?, ?, ?, ?, ?, ?, ?);", tuple(row))
        self.connection.commit()

    def add_column(self, column_name, column_type):
        self.cursor.execute(f"ALTER TABLE FlightLeg ADD {column_name} {column_type}")
        self.connection.commit()

    def fill_in_travel_time(self):
        self.cursor.execute("""UPDATE FlightLeg SET flightDuration = ROUND((JULIANDAY(landingTimeUtc) - JULIANDAY(departureTimeUtc))*1440)""")
        self.connection.commit()

    def fill_in_flight_type(self):
        self.cursor.execute("""
        UPDATE FlightLeg SET flightType = CASE WHEN sourceCountryCode = destinationCountryCode THEN 'D' ELSE 'I' END""")
        self.connection.commit()

    def most_flights(self):
        self.cursor.execute(f"SELECT tailNumber, COUNT(tailNumber) AS Flights FROM FlightLeg GROUP BY tailNumber ORDER BY Flights DESC LIMIT 1")
        flights_result = self.cursor.fetchall()
        print(f'\nPlane with tail number {flights_result[0][0]} was the most active and made {flights_result[0][1]} flights.')

    def most_time_in_the_sky(self):
        self.cursor.execute(f"SELECT tailNUmber, SUM(flightDuration) AS Time FROM FlightLeg GROUP BY tailNumber ORDER BY Time DESC LIMIT 1")
        time_result = self.cursor.fetchall()
        print(f'\nPlane with tail number {time_result[0][0]} had the most time in the sky - {time_result[0][1]} minutes.')

    def shortest_and_longest_flights(self):
        self.cursor.execute(f"SELECT * FROM FlightLeg WHERE flightType LIKE 'I' ORDER BY flightDuration DESC LIMIT 1")
        long_int_flight = self.cursor.fetchall()
        self.cursor.execute(f"SELECT * FROM FlightLeg WHERE flightType LIKE 'I' ORDER BY flightDuration ASC LIMIT 1")
        short_int_flight = self.cursor.fetchall()
        self.cursor.execute(f"SELECT * FROM FlightLeg WHERE flightType LIKE 'D' ORDER BY flightDuration DESC LIMIT 1")
        long_dom_flight = self.cursor.fetchall()
        self.cursor.execute(f"SELECT * FROM FlightLeg WHERE flightType LIKE 'D' ORDER BY flightDuration ASC LIMIT 1")
        short_dom_flight = self.cursor.fetchall()
        print(f'\nThe longest international flight was the flight number {long_int_flight[0][0]} that took {long_int_flight[0][8]} minutes.')
        print(f'The shortest international flight was the flight number {short_int_flight[0][0]} that took {short_int_flight[0][8]} minutes.')
        print(f'The longest domestic flight was the flight number {long_dom_flight[0][0]} that took {long_dom_flight[0][8]} minutes.')
        print(f'The shortest domestic flight was the flight number {short_dom_flight[0][0]} that took {short_dom_flight[0][8]} minutes.')

    def invalid_records(self):
        self.cursor.execute(f"SELECT a.TailNumber, a.id, CASE WHEN ((a.departureTimeUtc > b.departureTimeUtc AND a.departureTimeUtc < b.landingTimeUtc) OR (a.departureTimeUtc < b.departureTimeUtc AND a.landingTimeUtc > b.landingTimeUtc) OR (a.landingTimeUtc > b.departureTimeUtc AND a.landingTimeUtc < b.landingTimeUtc) OR (a.departureTimeUtc > b.departureTimeUtc AND a.landingTimeUtc < b.landingTimeUtc)) THEN 'Invalid' ELSE 'Valid' END AS isValid FROM FlightLeg a INNER JOIN FlightLeg b ON a.tailNumber = b.tailNumber WHERE isValid LIKE 'Invalid'")
        flights_colliding = self.cursor.fetchall()
        print(f'\nThere are {len(flights_colliding)} invalid records (colliding flights):')
        for each in flights_colliding:
            print(each[1])

    def __del__(self):
        self.connection.close()

# creating an object
db = Database('flights.db')

# # importing data
# db.insert('flightlegs.csv')

# # adding new columns
# db.add_column('flightDuration', 'INTEGER')
# db.add_column('flightType', 'CHAR(1)')

# # adding data to created columns
# db.fill_in_travel_time()
# db.fill_in_flight_type()

# # calling a function that returns a plane which performed the most flights
# db.most_flights()

# # calling a function that returns a plane which had the most time in the sky
# db.most_time_in_the_sky()

# # calling a function that returns the shortest and longest flights divided into domestic and international
# db.shortest_and_longest_flights()

# # calling a function that returns a number of invalid records (colliding flights)
db.invalid_records()