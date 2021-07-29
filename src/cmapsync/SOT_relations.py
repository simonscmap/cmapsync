"""
Author: Norland Raphael Hagen <norlandrhagen@gmail.com>
Date: 07-29-2021

cmapsync - SOT Relations - source of Truth relations for Parent/child DB's
"""


### Source of Truth (SOT) Parent Child Table Relations
db = "opedia"
Parent = "Rainier"
Children = ["Mariana","Rossby"]


### Email Report Address ###

curator_email = "{INSERT EMAIL}
gmail_addr = "{INSERT GMAIL ADDRESS FOR SENDING}

### Report Directory ###
report_dir = "../../reports/"
report_name = "CMAP_DB_sync_report.csv"
table_specific_reports_dir = report_dir + "table_specific_reports/"
