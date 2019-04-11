"""
Write a query that, when given a lead generation daily summary table,
returns the customer(s) that generated the most total leads for May, 2018.

tl;dr
    WITH aggregate_totals AS (
        SELECT
            client_id,
            SUM(number_of_leads) leads_agg
        FROM lead_daily_sum
        WHERE (
            sum_date >= DATE('2018-05-01') AND
            sum_date < DATE('2018-06-01')
        )
        GROUP BY client_id
    )
    SELECT client_id [leads_agg]
    FROM aggregate_totals
    WHERE leads_agg = (
        SELECT MAX(leads_agg)
        FROM aggregate_totals
    )

Notes:
    * The test contained a mismatch in the data between the DML and sample
      table. I favor the DML here.
    * Lots of print statements below - to make it easy to run via console and
      see what's going on.
    * Much of the code below deals with reading data from a csv file into a
      local sqlite db, and running the query above against it. The code includes
      demonstrations of common mechanics like parameterized queries, data validation
      etc. I took a bunch of shortcuts - each ingest/query/aggregation process in
      a large scale production system is tuned separately.
"""
from calendar import monthrange
from csv import DictReader
import datetime
import logging
import sqlite3


def convert_date_for_db(input_date):
    """
    Convert date to yyyy-mm-dd format since sqlite3 will not.
    """
    try:
        dt = datetime.datetime.strptime(
            str(input_date),
            SAMPLE_DATA_DATE_FORMAT
        )
    except ValueError:
        message = "data_format_unrecognized: {}".format(input_date)
        logging.error(message)
        raise

    return dt.strftime("%Y-%m-%d")


class ClientQuery:
    def __init(self):
        self.conn = None
        self.cursor = None

    def _ready_db(self):
        self.conn = sqlite3.connect(SQLITE_FILE)
        self.cursor = self.conn.cursor()

    def _ready_source_table(self):
        """
        Create an sqlite table `lead_daily_sum` if not found.
        """
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS lead_daily_sum (
                client_id integer NOT NULL,
                sum_date date NOT NULL,
                number_of_leads integer,
                UNIQUE (client_id, sum_date) ON CONFLICT REPLACE
            );
        ''')

        self.conn.commit()

    def _ready_source_data(self):
        """
        Populate the table `lead_daily_sum` using a CSV source.
        """
        print(f"Reading source data from {SAMPLE_DATA_CSV}")
        with open(SAMPLE_DATA_CSV, "r") as f:
            sql_insert_values = [
                (
                    row['client_id'],
                    convert_date_for_db(row["sum_date"]),
                    row["number_of_leads"]
                )
                for row in DictReader(f, quotechar="'")
            ]

        self.cursor.executemany('''
            INSERT INTO lead_daily_sum (
                client_id,
                sum_date,
                number_of_leads
            )
            VALUES (?, ?, ?);
        ''', sql_insert_values)

        self.conn.commit()

    def _close_db(self):
        self.conn.close()

    def get_monthly_lead_leader(self, month, year):
        """
        Get the client_id(s) of the lead leader(s) for a specified month.

        :param month: integer representing the month (january = 1)
        :param year: 4-digit integer representing the year

        :return: list of one or more (in case of a tie) client ids
        """
        self._ready_db()
        self._ready_source_table()
        self._ready_source_data()

        year_and_month = f"{year}-{month:02}"
        start_date = f"{year_and_month}-01"
        end_date = f"{year_and_month}-{monthrange(year, month)[1]}"

        self.cursor.execute('''
            WITH aggregate_totals AS (
                SELECT
                    client_id,
                    SUM(number_of_leads) leads_agg
                FROM lead_daily_sum
                WHERE (
                    sum_date >= DATE(?) AND
                    sum_date <= DATE(?)
                )
                GROUP BY client_id
            )
            SELECT client_id [leads_agg]
            FROM aggregate_totals
            WHERE leads_agg = (
                SELECT MAX(leads_agg)
                FROM aggregate_totals
            )
        ''', (start_date, end_date))

        print(f"Customers who generated the most leads in {month}/{year}.")
        result = list(self.cursor)
        for row in result:
            print(f"Client ID: {row[0]}")

        self._close_db()
        return result


if __name__ == "__main__":

    SQLITE_FILE = "./sqlite.db"
    SAMPLE_DATA_CSV = "./data/lead_daily_sum_sample_data.csv"
    SAMPLE_DATA_DATE_FORMAT = "%d-%b-%Y"

    cq = ClientQuery()
    cq.get_monthly_lead_leader(5, 2018)
