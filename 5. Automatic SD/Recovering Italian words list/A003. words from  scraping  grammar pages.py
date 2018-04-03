
__author__ = 'mirko'
import pymysql
import config as cfg
import urllib.request
import urllib.parse
from bs4 import BeautifulSoup
import re
import string


db = pymysql.connect(host=cfg.mysql['host'], # your host, usually localhost
             user=cfg.mysql['user'], # your username
             passwd=cfg.mysql['passwd'], # your password
             db=cfg.mysql['db'],
             charset='utf8') # name of the data base

cur = db.cursor()
cur.execute('SET NAMES utf8mb4')
cur.execute("SET CHARACTER SET utf8mb4")
cur.execute("SET character_set_connection=utf8mb4")
db.commit()


def cleanhtml(raw_html):
  cleanr = re.compile('<.*?>')
  cleantext = re.sub(cleanr, '', raw_html)
  return cleantext


def savewords(language, page,deep):
    print(page,deep)
    if deep<1:
        webURL = urllib.request.urlopen(page)
        data = webURL.read()
        encoding = webURL.info().get_content_charset('utf-8')

        result=data.decode(encoding)

        soup = BeautifulSoup(result) # I saved the webpage
        content = soup.find('div',{'id':'content'})
        bodyContent =    content.find('div',{'id':'bodyContent'})
        mwcontenttext =    bodyContent.find('div',{'id':'mw-content-text'})

        tables=mwcontenttext.findAll('table',{'class':'wikitable'})
        text=""

        for table in tables:
            print(table)
            tds= table.findAll('td')
            print(tds)
            for td in tds:
                print(td)
                text=text+" "+cleanhtml(str(td))
        print(text)

        include = set(string.ascii_letters)|set(" ")
        text = ''.join(ch for ch in text if ch in include)
        for word in text.lower().split(" "):

            if not word.isdigit() and len(word)>0:

                cur.execute("""
                INSERT INTO `dictionary`(`word`, `language`)
                VALUES
                (%s,%s)
                on duplicate key update frequency=frequency+1
                """,(word,language))

                db.commit()

        return



pages={

    "it":"https://it.wikipedia.org/wiki/Grammatica_italiana",


}


for language, page in pages.items():
    savewords(language, page,0)
