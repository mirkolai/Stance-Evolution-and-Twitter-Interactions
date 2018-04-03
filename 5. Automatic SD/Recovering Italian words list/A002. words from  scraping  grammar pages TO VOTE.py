
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


def savewords(language, page):
    webURL = urllib.request.urlopen(page)
    data = webURL.read()
    encoding = webURL.info().get_content_charset('utf-8')

    result=data.decode(encoding)

    soup = BeautifulSoup(result) # I saved the webpage
    contents = soup.findAll('span',{'class':'conjugation-level1'})
    print(contents)
    for content in contents:

        print(language,content.text)
        if not content.text.isdigit() and len(content.text)>0:

            cur.execute("""
            INSERT INTO `dictionary`(`word`, `language`)
            VALUES
            (%s,%s)
            on duplicate key update frequency=frequency+1
            """,(content.text,language))

            db.commit()

            cur.execute("""
            INSERT INTO `dictionary_tovote`(`word`, `language`)
            VALUES
            (%s,%s)
            on duplicate key update frequency=frequency+1
            """,(content.text,language))

            db.commit()


    return



pages={

    "it":"http://it.bab.la/coniugazione/italiano/votare",


}


for language, page in pages.items():
    savewords(language, page)
