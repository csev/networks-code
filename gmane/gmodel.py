import sqlite3
import time
import urllib
from urlparse import urljoin
from urlparse import urlparse
from BeautifulSoup import *
import re
import dateutil.parser as parser


conn = sqlite3.connect('index.sqlite')
cur = conn.cursor()

cur.execute('''DROP TABLE IF EXISTS Messages ''')
cur.execute('''DROP TABLE IF EXISTS Senders ''')
cur.execute('''DROP TABLE IF EXISTS Subjects ''')
cur.execute('''DROP TABLE IF EXISTS Replies ''')

cur.execute('''CREATE TABLE IF NOT EXISTS Messages 
    (id INTEGER PRIMARY KEY, guid TEXT UNIQUE, sent_at INTEGER, 
     sender_id INTEGER, subject_id INTEGER, 
     headers TEXT, body TEXT)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Senders 
    (id INTEGER PRIMARY KEY, sender TEXT UNIQUE)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Subjects 
    (id INTEGER PRIMARY KEY, subject TEXT UNIQUE)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Replies 
    (from_id INTEGER, to_id INTEGER)''')

conn_1 = sqlite3.connect('content.sqlite')
cur_1 = conn_1.cursor()


cur_1.execute('''SELECT headers, body, sent_at 
    FROM Messages ORDER BY sent_at''')

senders = dict()
subjects = dict()
guids = dict()

for message_row in cur_1 :
    # Parse out the info...
    hdr = message_row[0]
    sender = None
    x = re.findall('\nFrom: .* <(\S+@\S+)>\n', hdr)
    if len(x) >= 1 :
        sender = x[0].strip()
    else:
        x = re.findall('\nFrom: (\S+@\S+)\n', hdr)
        if len(x) >= 1 :
            sender = x[0].strip()

    date = None
    y = re.findall('\nDate: .*, (.*)\n', hdr)
    if len(y) >= 1 :
        tdate = y[0]
        tdate = tdate[:26]
        try:
            pdate = parser.parse(tdate)
            sent_at = pdate.isoformat()
        except:
            print text
            print "Parse fail",tdate
            break

    subject = None
    z = re.findall('\nSubject: (.*)\n', hdr)
    if len(z) >= 1 : subject = z[0].strip()

    guid = None
    z = re.findall('\nMessage-ID: (.*)\n', hdr)
    if len(z) >= 1 : guid = z[0].strip()

    print sender,sent_at,subject,guid
    if sender is None or sent_at is None or subject is None or guid is None :
        print hdr
        quit()
    sender_id = senders.get(sender,None)
    subject_id = subjects.get(subject,None)
    guid_id = guids.get(guid,None)
    if sender_id is None : 
        cur.execute('INSERT OR IGNORE INTO Senders (sender) VALUES ( ? )', ( sender, ) )
        conn.commit()
        cur.execute('SELECT id FROM Senders WHERE sender=? LIMIT 1', ( sender, ))
        try:
            row = cur.fetchone()
            sender_id = row[0]
            senders[sender] = sender_id
        except:
            print 'Could not retrieve sender id',sender
            break
    if subject_id is None : 
        cur.execute('INSERT OR IGNORE INTO Subjects (subject) VALUES ( ? )', ( subject, ) )
        conn.commit()
        cur.execute('SELECT id FROM Subjects WHERE subject=? LIMIT 1', ( subject, ))
        try:
            row = cur.fetchone()
            subject_id = row[0]
            subjects[subject] = subject_id
        except:
            print 'Could not retrieve subject id',subject
            break
    print sender_id, subject_id
    cur.execute('INSERT OR IGNORE INTO Messages (guid,sender_id,subject_id,headers,body) VALUES ( ?,?,?,?,? )', 
            ( guid, sender_id, subject_id, message_row[0], message_row[1]) )
    conn.commit()
    cur.execute('SELECT id FROM Messages WHERE guid=? LIMIT 1', ( guid, ))
    try:
        row = cur.fetchone()
        message_id = row[0]
        guids[guid] = message_id
    except:
        print 'Could not retrieve guid id',guid
        break

cur.close()
cur_1.close()

