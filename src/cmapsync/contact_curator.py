import yagmail
import SOT_relations as SOT
import datetime

curator_email = SOT.curator_email
body = "CMAP Database Report -- The folowing tables are out of sync:"
# filename = "document.pdf"

yag = yagmail.SMTP(SOT.curator_email)
yag.send(to=receiver, subject="CMAP Database Report", contents=body)
