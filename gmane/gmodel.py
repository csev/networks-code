import sqlite3
import time
import urllib
import re
import dateutil.parser as parser
import zlib

dnsmapping = dict()
mapping = dict()

def fixsender(sender,allsenders=None) :
    global dnsmapping
    global mapping
    if sender is None : return None
    sender = sender.strip().lower()
    sender = sender.replace('<','').replace('>','')

    # Check if we have a hacked gmane.org from address
    if allsenders is not None and sender.endswith('gmane.org') : 
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

    mpieces = sender.split("@")
    if len(mpieces) != 2 : return sender
    dns = mpieces[1]
    x = dns
    pieces = dns.split(".")
    if dns.endswith(".edu") or dns.endswith(".com") or dns.endswith(".org") :
        dns = ".".join(pieces[-2:])
    else:
        dns = ".".join(pieces[-3:])
    # if dns != x : print x,dns
    # if dns != dnsmapping.get(dns,dns) : print dns,dnsmapping.get(dns,dns)
    dns = dnsmapping.get(dns,dns)
    return mpieces[0] + '@' + dns

# Parse out the info...
def parseheader(hdr, allsenders=None):
    if hdr is None or len(hdr) < 1 : return None
    sender = None
    x = re.findall('\nFrom: .* <(\S+@\S+)>\n', hdr)
    if len(x) >= 1 :
        sender = x[0]
    else:
        x = re.findall('\nFrom: (\S+@\S+)\n', hdr)
        if len(x) >= 1 :
            sender = x[0]

    # normalize the domain name of Email addresses
    sender = fixsender(sender, allsenders)

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

cur_1.execute('''SELECT old,new FROM DNSMapping''')
for message_row in cur_1 :
    dnsmapping[message_row[0].strip().lower()] = message_row[1].strip().lower()

mapping = dict()
cur_1.execute('''SELECT old,new FROM Mapping''')
for message_row in cur_1 :
    old = fixsender(message_row[0])
    new = fixsender(message_row[1])
    mapping[old] = fixsender(new)

allsenders = list()
cur_1.execute('''SELECT email FROM Messages''')
for message_row in cur_1 :
    sender = fixsender(message_row[0])
    if sender is None : continue
    if 'gmane.org' in sender : continue
    if sender in allsenders: continue
    allsenders.append(sender)

print "Loaded allsenders",len(allsenders),"and mapping",len(mapping),"dns mapping",len(dnsmapping)

cur_1.execute('''SELECT headers, body, sent_at 
    FROM Messages ORDER BY sent_at''')

senders = dict()
subjects = dict()
guids = dict()

count = 0

for message_row in cur_1 :
    hdr = message_row[0]
    parsed = parseheader(hdr, allsenders)
    if parsed is None: continue
    (guid, sender, subject, sent_at) = parsed
    
    # Apply the sender mapping
    sender = mapping.get(sender,sender)

    count = count + 1
    if count % 250 == 1 : print count,sent_at, sender
    # print guid, sender, subject, sent_at

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

