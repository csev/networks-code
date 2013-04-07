import sqlite3
import time
import urllib
import dateutil.parser as parser
import zlib

conn = sqlite3.connect('index.sqlite')
conn.text_factory = str
cur = conn.cursor()

cur.execute('SELECT id, sender FROM Senders')
senders = dict()
for message_row in cur :
    senders[message_row[0]] = message_row[1]

cur.execute('SELECT id, subject FROM Subjects')
subjects = dict()
for message_row in cur :
    subjects[message_row[0]] = message_row[1]

# cur.execute('SELECT id, guid,sender_id,subject_id,headers,body FROM Messages')
cur.execute('SELECT id, guid,sender_id,subject_id,sent_at FROM Messages')
messages = dict()
for message_row in cur :
    messages[message_row[0]] = (message_row[1],message_row[2],message_row[3],message_row[4])

print "Loaded messages=",len(messages),"subjects=",len(subjects),"senders=",len(senders)

print type(messages)

sendcounts = dict()
for (message_id, message) in messages.items():
    sender = message[1]
    sendcounts[sender] = sendcounts.get(sender,0) + 1

x = sorted(sendcounts, key=sendcounts.get, reverse=True)
for k in x:
    print senders[k], sendcounts[k]
    if sendcounts[k] < 10 : break
