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

    def __del__(self):
        self.connection.close()

db = Database('flights.db')
db.insert('flightlegs.csv')
db.add_column('flightDuration', 'INTEGER')
db.add_column('flightType', 'CHAR(1)')
db.fill_in_travel_time()
db.fill_in_flight_type()
