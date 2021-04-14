import yagmail
import pandas as pd
import SOT_relations as SOT
import datetime
import general


def read_warning_report():
    df = pd.read_csv(SOT.report_dir + SOT.report_name)
    return df


def send_warning(msg):
    body = msg
    yag = yagmail.SMTP(SOT.gmail_addr)
    yag.send(to=SOT.curator_email, subject="CMAP Database Report", contents=body)


def check_report():
    warning_df = read_warning_report()
    if warning_df.empty == False:
        send_warning(warning_df.to_markdown(index=False))


check_report()
