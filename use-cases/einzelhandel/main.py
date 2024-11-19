from mail_sender import send_email
from notebook_creator import create_notebook
from dotenv import load_dotenv
import json
import pandas as pd
load_dotenv()

def create_report_and_send():
    print("make sure that there are no empty cells in the ac_datep_vis.ipynb notebook!")

    # enter parameters here
    year = 2024
    month = 10  # enter number between 1-12

    # get month name
    month_str = ['Januar', 'Februar', 'März', 'April', 'Mai', 'Juni', 'Juli', 'August', 'September', 'Oktober',
                 'November', 'Dezember'][month - 1]

    # get box data:
    boxes = pd.read_csv('use-cases/einzelhandel/einzelhandel.csv', sep = ";", header = 2, encoding='latin-1')
    # boxes = pd.read_csv('einzelhandel.csv', sep = ";", header = 2, encoding='latin-1')

    for index, box in boxes.iterrows():
        
        output_filename = f"Besucher_Bericht_{month_str}_{box['Box-ID']}"

        # run notebook and convert to pdf
        created = create_notebook(year, month, month_str, box["Box-ID"], box["Standort"], box["Unternehmen"], output_filename)

        if created:
            # send pdf to emails
            send_email(box["E-Mail"], box["Ansprechpartner"], month_str, output_filename)

        print("Done.")


if __name__ == '__main__':
    create_report_and_send()
