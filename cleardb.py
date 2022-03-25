import sqlite3

conn = sqlite3.connect("yamm.db")
cursor = conn.cursor()


def clearSentEmails():
    cursor.execute("DELETE FROM Sent_Emails")


option = input("Option: ")
if option == "Sent_Emails":
    print("Clearing Sent_Emails")
    clearSentEmails()

conn.commit()
conn.close()
