import sqlite3
import time
import urllib
from urlparse import urljoin
from urlparse import urlparse
import re
import dateutil.parser as parser

conn = sqlite3.connect('content.sqlite')
cur = conn.cursor()
conn.text_factory = str

baseurl = "http://download.gmane.org/gmane.comp.cms.sakai.devel/"

cur.execute('''CREATE TABLE IF NOT EXISTS Messages 
    (id INTEGER UNIQUE, email TEXT, sent_at TEXT, 
     subject TEXT, headers TEXT, body TEXT)''')

# This will be manually filled in
cur.execute('''CREATE TABLE IF NOT EXISTS Mapping 
    (old TEXT, new TEXT)''')

# This will be manually filled in
cur.execute('''CREATE TABLE IF NOT EXISTS DNSMapping 
    (old TEXT, new TEXT)''')

start = 0
many = 0
while True:
    if ( many < 1 ) :
        sval = raw_input('How many messages:')
        if ( len(sval) < 1 ) : break
        many = int(sval)

    start = start + 1
    cur.execute('SELECT id FROM Messages WHERE id=?', (start,) )
    try:
        row = cur.fetchone()
        if row is not None : continue
    except:
        row = None
        
    many = many - 1
    url = baseurl + str(start) + '/' + str(start + 1)

    try:
        document = urllib.urlopen(url)
        text = document.read()
        if document.getcode() != 200 :
            print "Error code=",document.getcode(), url
            break
    except KeyboardInterrupt:
        print ''
        print 'Program interrupted by user...'
        break
    except:
        print "Unable to retrieve or parse page",url
        break

    print url,len(text)

    if not text.startswith("From "):
        print text
        print "Does not start with From "
        quit ()

    pos = text.find("\n\n")
    if pos > 0 : 
        hdr = text[:pos]
        body = text[pos+2:]
    else:
        print text
        print "Could not find break between headers and body"
        break

    email = None
    x = re.findall('\nFrom: .* <(\S+@\S+)>\n', hdr)
    if len(x) == 1 : 
        email = x[0];
        email = email.strip().lower()
        email = email.replace("<","")
    else:
        x = re.findall('\nFrom: (\S+@\S+)\n', hdr)
        if len(x) == 1 : 
            email = x[0];
            email = email.strip().lower()
            email = email.replace("<","")

    date = None
    y = re.findall('\Date: .*, (.*)\n', hdr)
    if len(y) == 1 : 
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
    z = re.findall('\Subject: (.*)\n', hdr)
    if len(z) == 1 : subject = z[0].strip().lower();

    print "   ",email,sent_at,subject
    cur.execute('''INSERT OR IGNORE INTO Messages (id, email, sent_at, subject, headers, body) 
        VALUES ( ?, ?, ?, ?, ?, ? )''', ( start, email, sent_at, subject, hdr, body))
    conn.commit()
    time.sleep(1)

cur.close()

