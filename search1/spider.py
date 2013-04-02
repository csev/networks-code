import sqlite3
import urllib
from urlparse import urljoin
from urlparse import urlparse
from BeautifulSoup import *

conn = sqlite3.connect('spider.sqlite')
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS Pages 
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT, 
     error INTEGER, old_rank REAL, new_rank REAL)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Links 
    (from_id INTEGER, to_id INTEGER)''')

starturl = raw_input('Enter web url or enter: ')
if ( len(starturl) < 1 ) : starturl = 'http://www.dr-chuck.com/'
if ( starturl.endswith('/') ) : starturl = starturl[:-1]

if ( len(starturl) > 1 ) :
    cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( starturl, ) ) 
    conn.commit()

many = 0
while True:
    if ( many < 1 ) :
        sval = raw_input('How many pages:')
        if ( len(sval) < 1 ) : break
        many = int(sval)
    many = many - 1

    cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL LIMIT 1')
    try:
        row = cur.fetchone()
        # print row
        fromid = row[0]
        url = row[1]
    except:
        print 'No unretrieved HTML pages found'
        many = 0
        break

    print fromid, url, 

    # If we are retrieving this page, there should be no links from it
    cur.execute('DELETE from Links WHERE from_id=?', (fromid, ) )
    try:
        document = urllib.urlopen(url)
        html = document.read()
        if document.getcode() != 200 :
            cur.execute('UPDATE Pages SET error=? WHERE url=?', (document.getcode(), url) )
        if 'text/html' != document.info().gettype() :
            print "Ignore non text/html page"
            cur.execute('DELETE FROM Pages WHERE url=?', ( url, ) ) 
            cur.execute('UPDATE Pages SET error=0 WHERE url=?', (url, ) )
            conn.commit()
            continue

        soup = BeautifulSoup(html)
    except KeyboardInterrupt:
        print ''
        print 'Program interrupted by user...'
        break
    except:
        print "Unable to retrieve or parse page"
        cur.execute('UPDATE Pages SET error=-1 WHERE url=?', (url, ) )
        conn.commit()
        continue

    cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( url, ) ) 
    cur.execute('UPDATE Pages SET html=? WHERE url=?', (buffer(html), url ) )
    conn.commit()

    # Retrieve all of the anchor tags
    tags = soup('a')
    count = 0
    for tag in tags:
        href = tag.get('href', None)
        if ( href is None ) : continue
        # Resolve relative references like href="/contact"
        up = urlparse(href)
        if ( len(up.scheme) < 1 ) :
            href = urljoin(url, href)
        if ( not href.startswith(starturl) ) : continue
        ipos = href.find('#')
        if ( ipos > 1 ) : href = href[:ipos]
        if ( href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif') ) : continue
        if ( href.endswith('/') ) : href = href[:-1]
        # print href
        if ( len(href) < 1 ) : continue

        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_rank) VALUES ( ?, NULL, 1.0 )', ( href, ) ) 
        count = count + 1
        conn.commit()

        cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', ( href, ))
        try:
            row = cur.fetchone()
            toid = row[0]
        except:
            print 'Could not retrieve id'
            continue
        # print fromid, toid
        cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', ( fromid, toid ) ) 


    print count

cur.close()

