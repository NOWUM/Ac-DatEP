from datetime import datetime
import calendar
import json
import os
from db_service import connect, check_ds_data

def add_one_month(orig_date):
    # advance year and month by one month
    new_year = orig_date.year
    new_month = orig_date.month + 1
    # note: in datetime.date, months go from 1 to 12
    if new_month > 12:
        new_year += 1
        new_month -= 12

    last_day_of_month = calendar.monthrange(new_year, new_month)[1]
    new_day = min(orig_date.day, last_day_of_month)

    return orig_date.replace(year=new_year, month=new_month, day=new_day)

def make_arguments(year, month, month_str, sensorbox_id, address, name, output_file):
    now = datetime.now()
    
    start_year = year
    start_month = month
    start_day = 1
    start_hour = 0
    
    start_date = datetime(start_year,start_month,start_day,start_hour)   
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date = add_one_month(start_date)
    end_date_str = end_date.strftime("%Y-%m-%d")

    month_str = ['Januar', 'Februar', 'März', 'April', 'Mai', 'Juni', 'Juli', 'August', 'September', 'Oktober',
                 'November', 'Dezember'][start_month - 1]
    end_year = end_date.year
    end_month = end_date.month
    end_day = end_date.day
    end_hour = end_date.hour

    arguments = {
      "start_year": start_year,
      "start_month": start_month,
      "start_day": start_day,
      "start_hour": start_hour,
      "end_year": end_year,
      "end_month": end_month,
      "end_day": end_day,
      "end_hour": end_hour,
      "start_date": start_date_str,
      "end_date": end_date_str,
      "month_str": month_str, 
      "sensor_id": sensorbox_id,
      "name": name,
      "adresse":address
    }
    with open('use-cases/einzelhandel/arguments.json', 'w') as fid:
        json.dump(arguments, fid)
    return arguments

def create_notebook(year, month, month_str, sensorbox_id, address, name, output_file):
    # Create parameter file for jupyter-notebook

    arguments = make_arguments(year, month, month_str, sensorbox_id, address, name, output_file)
    con = connect()
    data_exists = check_ds_data(con, sensorbox_id, arguments["start_date"], arguments["end_date"])

    if data_exists:
        print("Creating PDF...")

        # Run the notebook
        os.system(f"jupyter nbconvert --execute --to webpdf --no-input ./use-cases/einzelhandel/einzelhandelsbericht.ipynb --log-level 40 --output {output_file}")

        input("Check pdf and press Enter to continue...")
        return True
    else:
        print("No data for sensor box {sensorbox_id}. No file is created.")
        return False
