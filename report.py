from collections import defaultdict
from datetime import datetime, timedelta, time

import pytz
import pandas as pd

from db import postgres_client


def generate_report():
    """
    Fetch everything from all 3 tables
    Loop through each store and do:
    Convert menu_hours to appropriate timezone and bifurcate by weekday
    Calculate max time_stamp or end time to consider for a store
    Go through each business hour interval of each day and calculate uptime and downtime
    Store in results
    """

    UTC = pytz.timezone("UTC")
    conn = postgres_client
    cursor = conn.cursor()
    results = [
        "store_id,uptime_last_hour,uptime_last_day,update_last_week,downtime_last_hour,downtime_last_day,downtime_last_week"
    ]

    cursor.execute("SELECT * from store_status ;")
    df = pd.DataFrame((cursor.fetchall()), columns=["store_id", "status", "timestamp"])
    store_ids = df["store_id"].unique()

    cursor.execute("SELECT * from store_timezone;")
    store_timezone = {row[0]: row[1] for row in cursor.fetchall()}

    menu_hours = defaultdict(lambda: defaultdict(list))
    cursor.execute("SELECT * from menu_hours ;")
    for row in cursor.fetchall():
        store, day, start, end = row
        timezone = pytz.timezone(store_timezone.get(store, "America/Chicago"))
        start = time.fromisoformat(start).replace(tzinfo=timezone)
        end = time.fromisoformat(end).replace(tzinfo=timezone)
        menu_hours[store][int(day)].append([start, end])

    for store in store_ids:
        timezone = pytz.timezone(store_timezone.get(store, "America/Chicago"))
        this_store = df[df["store_id"] == store]
        this_store["timestamp"] = this_store["timestamp"].apply(
            lambda x: x.replace(tzinfo=UTC).astimezone(timezone)
        )

        business_hours = menu_hours[store]
        if not business_hours:
            for i in range(7):
                business_hours[i].append(
                    [
                        time.fromisoformat("00:00:00").replace(tzinfo=timezone),
                        time.fromisoformat("23:59:59").replace(tzinfo=timezone),
                    ]
                )

        for i in range(7):
            business_hours[i].sort()

        # THIS SHOULD BE FROM FILTERED
        max_time = this_store["timestamp"].max()
        weekday = max_time.weekday()

        if weekday in business_hours and business_hours[weekday]:
            if max_time < datetime.combine(max_time, business_hours[weekday][0][0]):
                # If last ping was before business hours of same day, then set to end of previous day
                max_time = max_time - timedelta(days=1)
                weekday = max_time.weekday()
                while weekday not in business_hours or not business_hours[weekday]:
                    max_time = max_time - timedelta(days=1)
                    weekday = max_time.weekday()
                max_time = datetime.combine(max_time, business_hours[weekday][-1][1])
            elif max_time > datetime.combine(max_time, business_hours[weekday][-1][1]):
                # If last ping was after business hours of same day, set to end of business hours
                max_time = datetime.combine(max_time, business_hours[weekday][-1][1])
            else:
                # If last ping is during a break in business hours, set to previous end of business hours
                last_time = datetime.combine(max_time, business_hours[weekday][0][1])
                for start, end in business_hours[weekday]:
                    if max_time > datetime.combine(max_time, end):
                        continue
                    if max_time < datetime.combine(max_time, start):
                        max_time = last_time
                        break
                    last_time = datetime.combine(max_time, end)

        seconds_in_hour, seconds_in_day, seconds_in_week = (
            60 * 60,
            60 * 60 * 24,
            60 * 60 * 24 * 7,
        )
        current_minus_hour = max_time - timedelta(seconds=seconds_in_hour)
        current_minus_day = max_time - timedelta(seconds=seconds_in_day)
        current_minus_week = max_time - timedelta(seconds=seconds_in_week)
        uptime_hour, uptime_day, uptime_week = 0, 0, 0
        downtime_hour, downtime_day, downtime_week = 0, 0, 0
        last = max_time
        accumulated_time = 0

        for day in business_hours:
            for start, end in business_hours[day]:
                filtered = this_store[
                    (start <= this_store["timestamp"].dt.time)
                    & (this_store["timestamp"].dt.time <= end)
                    & (this_store["timestamp"].dt.dayofweek == day)
                ].values.tolist()
                filtered.sort(key=lambda x: x[2], reverse=True)

                if not filtered:
                    continue
                start_with_date = datetime.combine(filtered[-1][2], start)
                end_with_date = datetime.combine(filtered[0][2], end)
                last = min(max_time, end_with_date)
                filtered.append([store, filtered[-1][1], start_with_date])

                for row in filtered:
                    _, status, timestamp = row
                    if (max_time - timestamp).total_seconds() <= seconds_in_hour:
                        seconds = (last - timestamp).total_seconds()
                        if timestamp < current_minus_hour:
                            non_overlap = (
                                current_minus_hour - timestamp
                            ).total_seconds()
                            seconds -= non_overlap

                        if status == "active":
                            uptime_hour += seconds
                        if status == "inactive":
                            downtime_hour += seconds

                    if (max_time - timestamp).total_seconds() <= seconds_in_day:
                        seconds = (last - timestamp).total_seconds()
                        if timestamp < current_minus_day:
                            non_overlap = (
                                current_minus_day - timestamp
                            ).total_seconds()
                            seconds -= non_overlap

                        if status == "active":
                            uptime_day += seconds
                        if status == "inactive":
                            downtime_day += seconds

                    if (max_time - timestamp).total_seconds() <= seconds_in_week and last.day == timestamp.day:
                        seconds = (last - timestamp).total_seconds()
                        if timestamp < current_minus_week:
                            non_overlap = (
                                current_minus_week - timestamp
                            ).total_seconds()
                            seconds -= non_overlap

                        if status == "active":
                            uptime_week += seconds
                        if status == "inactive":
                            downtime_week += seconds

                    accumulated_time += (last - timestamp).total_seconds()
                    last = timestamp

        uptime_hour = round(uptime_hour / 60, 2)
        uptime_day = round(uptime_day / 3600, 2)
        uptime_week = round(uptime_week / 3600, 2)
        downtime_hour = round(downtime_hour / 60, 2)
        downtime_day = round(downtime_day / 3600, 2)
        downtime_week = round(downtime_week / 3600, 2)
        results.append(
            f"{store},{uptime_hour},{uptime_day},{uptime_week},{downtime_hour},{downtime_day},{downtime_week}"
        )

    return results
