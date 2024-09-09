from datetime import datetime
import locale
import logging
import time
from typing import List, Tuple
import os

import db_service

import requests
import bs4
from bs4 import BeautifulSoup
import pandas as pd


locale.setlocale(locale.LC_ALL, "de_DE.UTF-8")

logging_level_str = os.getenv("LOGGING_LEVEL", "ERROR")

def request_website(url: str) -> bytes | None:

    """
    Requests website for given URL with 5 attempts.

    Parameters:
    -----------------
        URL: str
            URL to website to request.

    Returns:
    -----------------
        website: bytes or None
            The website as bytes or None if request failed 5 times
    """

    logging.info("Requesting website")

    for attempt in range(5):

        try:
            # try to request and return content
            r = requests.get(url)
            r.raise_for_status()

            logging.info("Successfully requested website")

            return r.content
        
        except Exception as e:
            # catch exception, log error and try again in 5 seconds
            msg = f"Something went wrong requesting website on attempt {attempt}: {e}"
            msg += f"Retrying in 5 seconds."
            logging.warning(msg)
            time.sleep(5)

    # if function did not return r.content yet it means
    # it did not work in 5 attempts so we return None
    return None


def get_dates_title(highlight: bs4.element.Tag) -> Tuple[str, str]:

    """
    Splits highlights text into title and dates.

    Parameters:
    -----------------
        highlight: bs4.element.Tag
            Text from website containing event name and event dates

    Returns:
    -----------------
        dates, title: Tuple[str, str]
            Tuple containing dates string and name string
    """

    information = highlight.find_all(class_="p1")
    return information[0].text, information[1].text


def get_year(date):

    # if 4th to last character of the date string is
    # not a 2 (years 2000 to 2999)
    # there is no year given
    if date[-4] != "2":
        return None
    else:
        return int(date[-4:])


def convert_to_datetime(dates: str) -> Tuple[datetime, datetime]:

    """
    Converts date string into two dates with year.

    Parameters:
    -----------------
        dates: str
            The string containing the dates

    Returns:
    -----------------
        start_time, end_time
            The start and end times of the event as datetime.datetime objects
    """

    # if there is "-" in the string the event goes from start_date - end_date
    # so we split it
    if " – " in dates:
        start_date, end_date = dates.split(" – ")

    # otherwise the event only is one day, so start_date equals end_date equals dates
    else:
        start_date = dates
        end_date = dates

    # extra case for something like 07. - 09. September
    if len(start_date) == 3 and start_date[-1] == ".":

        # save start day (current start_date)
        start_day = start_date

        # set end date as start date
        start_date = end_date

        # replace first 3 chars of new start date with start day
        start_date = list(start_date)
        start_date[:3] = list(start_day)
        start_date = "".join(start_date)

    # if the year is missing from the string its the current year
    # checking for that
    start_year = get_year(start_date)
    end_year = get_year(end_date)

    # if start year is not given its same as end year (current)
    if not start_year:
        start_date += f" {end_year}"

    # convert start and end times to datetime
    start_time = datetime.strptime(start_date, "%d. %B %Y")
    end_time = datetime.strptime(end_date, "%d. %B %Y")

    return start_time, end_time


def get_events_from_website(website: bytes) -> pd.DataFrame:

    """
    Builds pandas.DataFrame from website.

    Parameters:
    -----------------
        website: bytes
            The website containing the events information

    Returns:
    -----------------
        events: pd.DataFrame
            DataFrame containing event_name,
            start_date, end_date, additional_info and
            confidential columns
    """

    logging.info("Fetching events from website")

    # convert to soup
    soup = BeautifulSoup(website, "html.parser")

    # find all highlights
    highlights = soup.find_all(
        class_="wpb_row vc_row-fluid vc_row inner_row vc_row-o-equal-height vc_row-flex vc_row-o-content-middle")

    rows = []
    # last "highlight" is always found wrong
    for highlight in highlights[:-1]:

        # get dates and title
        dates, title = get_dates_title(highlight)

        # convert dates
        start_date, end_date = convert_to_datetime(dates)

        # append crawled information to rows
        rows.append({
            "event_name": title,
            "start_date": start_date,
            "end_date": end_date,
            "additional_info": None,
            "confidential": False})

    logging.info("Successfully retrieved events from website")

    # build dataframe from rows
    df = pd.DataFrame(rows)

    # convert start_date and end_date columns from timestamp to date
    df["start_date"] = df["start_date"].dt.date
    df["end_date"] = df["end_date"].dt.date

    return df



def sleep_24_hours(start_time: float) -> None:
    """
    Excercises 24h sleeping in relation to given start_time
    """

    end_time = time.time()
    elapsed_time = end_time - start_time
    time_to_sleep = (24 * 60 * 60) - elapsed_time

    time.sleep(time_to_sleep)


def remove_duplicate_events(
        retrieved_events: pd.DataFrame,
        existing_events: List[Tuple]) -> List[Tuple] | int:

    """
    Removes duplicated events from retrieved (crawled) events.

    Parameters:
    -----------------
        retrieved_events: pd.DataFrame
            DataFrame containing the newly crawled events

        existing_events: List[Tuple]
            The events already stored in the database

    Returns:
    -----------------
        to_feed_into_db: List[Tuple]
            Data to feed into the database as List of Tuples.
    """

    try:

        # if there are no existing events we can convert the
        # whole DataFrame into the correct format for psycopg2
        if existing_events == []:
            return list(tuple(x) for x in retrieved_events.to_numpy())
        else:
            # convert dataframes to sets of tuples
            new_events = {tuple(x) for x in retrieved_events.to_numpy()}

            # get new events which are not in old events
            to_feed_into_db = [event for event in new_events if event not in existing_events]

            return to_feed_into_db

    except Exception as e:
        logging.error(f"Something went wrong removing duplicated events: {e}")
        return -1


if __name__ == "__main__":

    if logging_level_str == "INFO":
        logging_level = logging.INFO
    elif logging_level_str == "WARNING":
        logging_level = logging.WARNING
    elif logging_level_str == "ERROR":
        logging_level = logging.ERROR

    # logging
    logging.basicConfig(
        level=logging_level,
        format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
        datefmt='%d-%m-%Y %H:%M:%S')

    while True:

        start_time = time.time()

        # request website
        url = "https://www.aachen-tourismus.de/aachen-entdecken/events/"
        website = request_website(url)

        # try again in 24h if website could not be retrieved
        if not website:
            time.sleep(24 * 60 * 60)
            continue

        # get events as DataFrame from website
        retrieved_events = get_events_from_website(website)

        # connect to database
        con = db_service.connect_to_database()

        if con == -1:
            logging.error("Trying again tomorrow")
            sleep_24_hours(start_time)
            continue

        # create table for events
        result = db_service.create_table(con)

        if result == -1:
            logging.error("Trying again tomorrow")
            sleep_24_hours(start_time)
            continue

        # get existing events
        existing_events = db_service.get_existing_events(con)

        if existing_events == -1:
            logging.error("Trying again tomorrow")
            sleep_24_hours(start_time)

        # remove events already existing in database
        new_events = remove_duplicate_events(
            retrieved_events=retrieved_events,
            existing_events=existing_events)

        # if new_events returns -1 there is something wrong with the code
        # no need to restart or trying again as manual fix is needed
        if new_events == -1:
            logging.error("Something in the code is not working. Exiting.")
            break

        # if there are no new events: log that and crawl again tomorrow
        elif new_events == []:
            logging.info("No new events. Asking again tomorrow.")
            sleep_24_hours(start_time)
            continue

        # feed new events to database
        # result variable is for possible error code
        result = db_service.feed_to_database(con, new_events)

        # log error from writing to database and try again tomorrow
        if result == -1:
            logging.error("Trying again tomorrow")
            sleep_24_hours(start_time)
            continue

        # commit changes and close connection
        con.commit()
        con.close()

        logging.info("Succesfully crawled events and written to database")
        logging.info("Crawling again tomorrow")
        sleep_24_hours(start_time)
