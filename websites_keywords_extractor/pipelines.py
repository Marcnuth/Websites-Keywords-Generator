# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sqlite3, time, jieba, jieba.analyse, jieba.posseg 
import settings as conf

class SaveToSqlitePipeline(object):
    def process_item(self, item, spider):

        try:
            
            dbfile = '%s/%s' % (conf.PROJECT_PATH['data'], conf.SQLITE['file'])
            conn = sqlite3.connect(dbfile)
            cursor = conn.cursor()
            cursor.execute('INSERT INTO urls VALUES(?, ?)', (item['url'], int(time.time())))

            for word, flag in jieba.posseg.cut(item['content']):
                if len(word) < 2:
                    continue

                if flag in ['r', 'nr', 'eng', 'p', 'u', 'ul', 'y', 'm', 'o', 'e', 'f', 't', 'w']:
                    continue
                
                select = cursor.execute('SELECT * FROM keywords where word=?',(word,)).fetchall()
                if select:
                    cursor.execute('UPDATE keywords SET count=? where word=?', (select[0][1] + 1, word))
                else:
                    cursor.execute('INSERT INTO keywords VALUES(?, ?)', (word, 1))
            conn.commit()
            print 'FINISH %s' % item['url']
            
        finally:
            conn.close()

