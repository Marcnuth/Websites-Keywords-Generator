# -*- coding: utf-8 -*-
import scrapy, jieba, jieba.analyse, sqlite3, os, shutil, time
import websites_keywords_extractor.settings as conf
from websites_keywords_extractor.items import KeywordsItem
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from BeautifulSoup import BeautifulSoup as bs, Comment

class KeywordsSpider(CrawlSpider):
    name = "keywords"

    allowed_domains = conf.KEYWORD_EXTRACTOR['allow_domains']
    start_urls = conf.KEYWORD_EXTRACTOR['start_urls']

    rules = (
        #spider will follow this link if there is not callback
        Rule(LinkExtractor(allow=('.*', )), callback='parse_web', follow=True),
    )

    def __init__(self):
        CrawlSpider.__init__(self)
        #create database
        try :
            dbfile = '%s/%s' % (conf.PROJECT_PATH['data'], conf.SQLITE['file'])
            if os.path.exists(dbfile):
                moveto = '%s.%d' % (dbfile, int(time.time()))
                shutil.move(dbfile, moveto)
                print 'old db file %s is moved to %s.' % (dbfile, moveto)
            
            conn = sqlite3.connect(dbfile)
            cursor = conn.cursor()
            for table in conf.SQLITE['tables']:
                cursor.execute(table['sql'])

            conn.commit()
            print 'db initialization complete!'
            
        finally:
            conn.close()
    
    def parse_web(self, response):

        html = bs(response.body)
       
        [_i.extract() for _i in html.findAll(text=lambda text:isinstance(text, Comment))]
        [_i.extract() for _i in html.findAll(name=['link', 'script', 'meta', 'style', 'footer', 'head'])]

        item = KeywordsItem()
        item['url'] = response.url
        item['content'] = html.getText('\n').encode('utf-8')
        
        yield item
