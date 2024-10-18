from mail_sender import send_email
from notebook_creator import create_notebook
from dotenv import load_dotenv
import json
load_dotenv()

def create_report_and_send():
    print("make sure that there are no empty cells in the ac_datep_vis.ipynb notebook!")

    # enter parameters here
    year = 2024
    month = 8  # enter number between 1-12

    # get month name
    month_str = ['Januar', 'Februar', 'März', 'April', 'Mai', 'Juni', 'Juli', 'August', 'September', 'Oktober',
                 'November', 'Dezember'][month - 1]

    # get mock data:
    with open('use-cases/einzelhandel/boxes_mockdata.json', 'r') as fid:
        boxes = json.load(fid)

    for box in boxes:
        output_filename = f"Besucher_Bericht_{month_str}_{box['sensorbox_id']}"

        # run notebook and convert to pdf
        create_notebook(year, month, month_str, box["sensorbox_id"], box["address"], box["store_name"], output_filename)

        # send pdf to emails
        for receiver in box["receivers"]:
            send_email(receiver["email"], receiver["name"], month_str, output_filename)

        print("Done.")


if __name__ == '__main__':
    create_report_and_send()
