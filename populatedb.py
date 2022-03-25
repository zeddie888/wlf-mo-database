"""
Created by: Edward Zhang
Purpose of this script: read from csv file to populate yamm.db database

Warning: database is potentially vulnerable to SQL injections

"""

import pandas as pd
import sqlite3
import re

conn = sqlite3.connect("yamm.db")
cursor = conn.cursor()

f = input("csv file to read: ")
df = pd.read_csv(f"sheets/{f}")

# print(df)

""" PROCEDURE
0. TODO: cleanup database- ensure all schools are of appropriate type, etc.?
2. Check if school already exists. If so, then do not add new entry; else add new entry. Return the school id
3. Check if the recipient already exists. If so, then do not add new entry; else add new entry. Return recipient id
4. Make new Sent_Emails entry, check if exists first, get the sent email ID
5. YAMM_Results entry: if entry does not already exist, then add a new one. Otherwise, UPDATE

"""

##### HELPER METHODS #######


def determineType(school: str):
    school = school.lower()
    if(school.__contains__("high school")):
        return "high school"
    if(school.__contains__("middle school")):
        return "middle school"
    if(school.__contains__("college") or school.__contains__("university")):
        return "college"
    if(school.__contains__("elementary")):
        return "elementary school"
    if(school.__contains__("preschool") or school.__contains__("pre-k")):
        return "preschool"
    return "NA"


def getMemberId(name):
    getMemberId = f"""
      SELECT id FROM Members WHERE LOWER(full_name)="{name.lower()}";
    """
    cursor.execute(getMemberId)
    res = cursor.fetchall()
    return res[0][0]


def formatDate(date: str):
    # We are using ISO8601 text format: YYYY-MM-DD
    arr = date.split("/")
    return f"{arr[2]}-{arr[0]}-{arr[1]}"


def formatTime(time: str):
    # We are using ISO8601 format: HH:MM:SS
    # Edge cases: midnight = 00:00:00 = 12 AM, noon = 12:00:00 = 12 PM
    # Normal cases: 1:30 AM = 01:30:00, 1:30 PM = 13:30:00
    test = re.search("^([0-9]|[0-9]{2}):[0-9]{2} (AM|PM)$", time)
    if test:
        # good
        timeClean = re.sub(" (AM|PM)$", "", time).split(":")
        hour = int(timeClean[0])
        minute = int(timeClean[1])
        if time.__contains__("AM"):
            if hour == 12:
                hour = 0
        if time.__contains__("PM"):
            if hour == 12:
                hour = 12
            else:
                hour += 12
        if hour < 10:
            hour = f"0{hour}"
        return f"{hour}:{minute}:00"

    else:
        print("ERROR: time is not formatted correctly", time)
        exit()

        #############################


def addSchool(s):
    # TODO: compare with lower, fill database in normal case
    school = str(s["School Name"])
    schoolDistrict = str(s["School District Name"])
    state = str(s["State"])
    country = str(s["Country"])

    schoolAlreadyExists = f"""
      SELECT id FROM Schools WHERE LOWER(name)="{school.lower()}" AND LOWER(state)="{state.lower()}";
    """

    cursor.execute(schoolAlreadyExists)
    res = cursor.fetchall()
    schoolId = None
    if not res:  # res is empty => no match, false
        # add new entry to Schools
        schoolType = determineType(school)
        addNewSchool = f"""
          INSERT INTO Schools (name, type, school_district_name, state, country)
          VALUES ("{school}", "{schoolType}",
                  "{schoolDistrict}", "{state}", "{country}");
        """
        cursor.execute(addNewSchool)

        schoolId = cursor.lastrowid  # return id of last inserted row
    else:
        # print("DUPLICATE", res, school)
        schoolId = (res[0])[0]
        # TODO: UPDATE the database

    return schoolId


def addRecipient(s):
    email = str(s["Email"]).lower()
    firstName = str(s["First Name"])
    lastName = str(s["Last Name"])
    title = str(s["Title"])

    recipientAlreadyExists = f"""
      SELECT id FROM Recipients WHERE email_address="{email}" AND LOWER(first_name)="{firstName.lower()}";
    """

    cursor.execute(recipientAlreadyExists)
    res = cursor.fetchall()
    recipientId = None
    if not res:  # res is empty => no match, false
        # add new entry to Recipients
        addNewRecipient = f"""
          INSERT INTO Recipients (first_name, last_name, email_address, title)
          VALUES ("{firstName}", "{lastName}",
                  "{email}", "{title}");
        """
        cursor.execute(addNewRecipient)
        recipientId = cursor.lastrowid  # return id of last inserted row
    else:
        # print("DUPLICATE", res, email)
        recipientId = (res[0])[0]

    return recipientId


def addSentEmail(s, schoolId, recipientId):
    senderName = str(s["Your Name"])
    senderId = getMemberId(senderName)
    senderEmail = str(s["Your Email"]).lower()
    typeContact = str(s["Type"]).lower()

    sentEmailAlreadyExists = f"""
      SELECT id FROM Sent_Emails WHERE school_id={schoolId} AND recipient_id={recipientId} AND LOWER(type)="{typeContact}";
    """

    cursor.execute(sentEmailAlreadyExists)
    res = cursor.fetchall()
    sentEmailId = None
    if not res:
        # add new entry
        addNewSentEmail = f"""
          INSERT INTO Sent_Emails (member_id, member_email_address, school_id, recipient_id, type)
          VALUES ({senderId}, "{senderEmail}",
                  {schoolId}, {recipientId}, "{typeContact}");
        """
        cursor.execute(addNewSentEmail)
        sentEmailId = cursor.lastrowid  # return id of last inserted row
    else:
        sentEmailId = (res[0])[0]
    return sentEmailId


def addEmailVariables(s, sentEmailId):
    templateNum = int(s["Template Number"])
    titleIncluded = str(s["Title/Name Included?"]).lower() == "yes"
    title = str(s["Title"]).lower()
    title = title if titleIncluded else "NONE"

    dayOfWeek = str(s["Day of Week Sent"]).upper()
    # note: in csv view it shows up as #####, but is read correctly
    dateSent = str(s["Date Sent"]).replace(" ", "")
    timeSent = str(s["Time Sent (PST)"]).upper()

    dateSent = formatDate(dateSent)
    timeSent = formatTime(timeSent)

    emailVarAlreadyExists = f"""
      SELECT id FROM Email_Variables WHERE sent_email_id={sentEmailId} AND date_sent="{dateSent}";
    """
    cursor.execute(emailVarAlreadyExists)
    res = cursor.fetchall()
    emailVarId = None
    if not res:
        # add new entry
        addNewEmailVar = f"""
          INSERT INTO Email_Variables (sent_email_id, email_template_num, title_included,
                                       day_of_week, date_sent, time_sent)
          VALUES ({sentEmailId}, {templateNum},
                  "{title}", "{dayOfWeek}", "{dateSent}", "{timeSent}");
        """
        cursor.execute(addNewEmailVar)
        emailVarId = cursor.lastrowid  # return id of last inserted row
    else:
        # print("DUPLICATE")
        emailVarId = (res[0])[0]
    return emailVarId


def addYAMMResult(s, sentEmailId):
    mergeStatus = str(s["Merge Status"]).upper()
    yammResultAlreadyExists = f"""
      SELECT * FROM YAMM_Results WHERE sent_email_id={sentEmailId};
    """
    cursor.execute(yammResultAlreadyExists)
    res = cursor.fetchall()
    yammId = None

    results = {
        "opened": 0,
        "clicked": 0,
        "responded": 0,
        "bounced": 0
    }
    if mergeStatus == "RESPONDED":
        results["responded"] = 1
        results["opened"] = 1
    elif mergeStatus == "EMAIL_OPENED":
        results["opened"] = 1
    elif mergeStatus == "BOUNCED":
        results["bounced"] = 1
    elif mergeStatus == "EMAIL_CLICKED":
        results["clicked"] = 1
        results["opened"] = 1

    if not res:
        # insert a new entry
        addNewYammResult = f"""
          INSERT INTO YAMM_Results (sent_email_id, opened, link_clicked, responded, bounced)
          VALUES ({sentEmailId}, "{results["opened"]}",
                  "{results["clicked"]}", "{results["responded"]}", "{results["bounced"]}");
        """
        cursor.execute(addNewYammResult)
        yammId = cursor.lastrowid  # return id of last inserted row
    else:
        # update
        yammId = (res[0])[0]
        if res[0][2] == 1:
            results["opened"] = 1
        if res[0][3] == 1:
            results["clicked"] = 1
        if res[0][4] == 1:
            results["responded"] = 1
        if res[0][5] == 1:
            results["bounced"] = 1
        updateYammResult = f"""
          UPDATE YAMM_Results SET opened="{results["opened"]}", link_clicked="{results["clicked"]}",
                                  responded="{results["responded"]}", bounced="{results["bounced"]}"
          WHERE id={yammId};
        """
        cursor.execute(updateYammResult)
    return yammId


for i in range(len(df.index)):
    s = df.loc[i]  # get a row
    schoolId = addSchool(s)
    recipientId = addRecipient(s)
    sentEmailId = addSentEmail(s, schoolId, recipientId)
    emailVarsId = addEmailVariables(s, sentEmailId)
    yammResultId = addYAMMResult(s, sentEmailId)

    # a = getMemberId("edward zhang")
    # print(a)


conn.commit()
conn.close()


"""
TODO:
1. Should update database even if entry is already present, not skip
"""
