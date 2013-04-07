import sqlite3
import time
import urllib
import re
import dateutil.parser as parser
import zlib

# Parse out the info...
def parseheader(hdr):
    if hdr is None or len(hdr) < 1 : return None
    sender = None
    x = re.findall('\nFrom: .* <(\S+@\S+)>\n', hdr)
    if len(x) >= 1 :
        sender = x[0].strip().lower()
        sender = sender.replace('<','').replace('>','')
    else:
        x = re.findall('\nFrom: (\S+@\S+)\n', hdr)
        if len(x) >= 1 :
            sender = x[0].strip()
            sender = sender.replace('<','').replace('>','')

    date = None
    y = re.findall('\nDate: .*, (.*)\n', hdr)
    sent_at = None
    if len(y) >= 1 :
        tdate = y[0]
        tdate = tdate[:26]
        try:
            pdate = parser.parse(tdate)
            sent_at = pdate.isoformat()
        except:
            return None

    subject = None
    z = re.findall('\nSubject: (.*)\n', hdr)
    if len(z) >= 1 : subject = z[0].strip().lower()

    guid = None
    z = re.findall('\nMessage-ID: (.*)\n', hdr)
    if len(z) >= 1 : guid = z[0].strip().lower()

    if sender is None or sent_at is None or subject is None or guid is None :
        return None
    return (guid, sender, subject, sent_at)

conn = sqlite3.connect('index.sqlite')
conn.text_factory = str
cur = conn.cursor()

cur.execute('''DROP TABLE IF EXISTS Messages ''')
cur.execute('''DROP TABLE IF EXISTS Senders ''')
cur.execute('''DROP TABLE IF EXISTS Subjects ''')
cur.execute('''DROP TABLE IF EXISTS Replies ''')

cur.execute('''CREATE TABLE IF NOT EXISTS Messages 
    (id INTEGER PRIMARY KEY, guid TEXT UNIQUE, sent_at INTEGER, 
     sender_id INTEGER, subject_id INTEGER, 
     headers BLOB, body BLOB)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Senders 
    (id INTEGER PRIMARY KEY, sender TEXT UNIQUE)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Subjects 
    (id INTEGER PRIMARY KEY, subject TEXT UNIQUE)''')
cur.execute('''CREATE TABLE IF NOT EXISTS Replies 
    (from_id INTEGER, to_id INTEGER)''')

conn_1 = sqlite3.connect('content.sqlite')
conn_1.text_factory = str
cur_1 = conn_1.cursor()

allsenders = list()
cur_1.execute('''SELECT email FROM Messages''')
for message_row in cur_1 :
    sender = message_row[0]
    if sender is None : continue
    sender = sender.lower()
    sender = sender.replace('<','').replace('>','')
    if 'gmane.org' in sender : continue
    if sender in allsenders: continue
    allsenders.append(sender)

mapping = dict()
cur_1.execute('''SELECT old,new FROM Mapping''')
for message_row in cur_1 :
    mapping[message_row[0].strip().lower()] = message_row[1].strip().lower()

print "Loaded allsenders",len(allsenders),"and mapping",len(mapping)

print mapping

cur_1.execute('''SELECT headers, body, sent_at 
    FROM Messages ORDER BY sent_at''')

senders = dict()
subjects = dict()
guids = dict()

for message_row in cur_1 :
    hdr = message_row[0]
    parsed = parseheader(hdr)
    if parsed is None: continue
    (guid, sender, subject, sent_at) = parsed

    # print guid, sender, subject, sent_at

    # Check if we have a hacked gmane.org from address
    if sender.endswith('gmane.org') : 
        pieces = sender.split('-')
        realsender = None
        for s in allsenders:
            if s.startswith(pieces[0]) :
                realsender = sender
                sender = s
                # print realsender, sender
                break
        if realsender is None : 
            for s in mapping:
                if s.startswith(pieces[0]) :
                    realsender = sender
                    sender = mapping[s]
                    # print realsender, sender
                    break
        if realsender is None : sender = pieces[0]

    # Check Mapping
    ## if mapping.get(sender,None) is not None:
        ## print "MAPPING=====",sender, mapping.get(sender,None)

    sender = mapping.get(sender,sender)

    if 'gmane.org' in sender:
        print "Error in sender ===", sender

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
    # print sender_id, subject_id
    cur.execute('INSERT OR IGNORE INTO Messages (guid,sender_id,subject_id,sent_at,headers,body) VALUES ( ?,?,?,datetime(?),?,? )', 
            ( guid, sender_id, subject_id, sent_at, zlib.compress(message_row[0]), zlib.compress(message_row[1])) )
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

