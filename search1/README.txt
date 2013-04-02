Simple Python Web Spider

This program crawls a web site and pulls a series of pages into the
database, recording the links between pages.
rm spider.sqlite

python spider.py 
Enter web url or enter: http://www.dr-chuck.com/
How many pages:2
1 http://www.dr-chuck.com/ 12
2 http://www.dr-chuck.com/csev-blog/ 57
How many pages:

In this sample run, we told it to crawl a website and retrieve two 
pages.  If you restart the program again and tell it to crawl more
pages, it will not re-crawl any pages already in the database.  Upon 
restart it goes to the top non-crawled page and starts there.  So 
each successive run of spider.py until you remove the database is additive.

python spider.py 
Enter web url or enter: http://www.dr-chuck.com/
How many pages:3
3 http://www.dr-chuck.com/csev-blog 57
4 http://www.dr-chuck.com/dr-chuck/resume/speaking.htm 1
5 http://www.dr-chuck.com/dr-chuck/resume/index.htm 13
How many pages:

You can view the database interactively using a Firefox plugin:

https://addons.mozilla.org/en-us/firefox/addon/sqlite-manager/

The plugin works more reliably than the desktop SQLiteBrowser.

You can run the following query to find broken links:

SELECT f.url, t.url,t.error FROM Pages AS f JOIN Links Join Pages as t ON f.id = from_id and to_id = t.id WHERE t.error > 0

Another interesting query is urls that caused the retrieval to fall into
an except block:

SELECT url FROM Pages where error < 0

