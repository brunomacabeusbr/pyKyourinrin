from . import Crawler
from selenium import webdriver
import re


class CrawlerG1(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_news_id INTEGER'
                        ');' % self.name())

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_news_id INTEGER,'
                            'name TEXT,'
                            'url TEXT'
                        ');' % (self.name() + '_tags'))

    @staticmethod
    def read_my_secondary_tables():
        return (
            {'table': 'tags'},
        )

    @staticmethod
    def column_export():
        def tags_list(read):
            return [{'name': i['name'], 'url': i['url']} for i in read['tags']]
        return (
            {'column_name': 'tags_list', 'how': tags_list},
        )
    
    @staticmethod
    def name():
        return 'g1'

    @staticmethod
    def dependencies():
        return '',

    @staticmethod
    def crop():
        return 'title', 'linha_fina', 'article', 'date_day', 'date_month', 'date_year', 'url'

    @staticmethod
    def primitive_required():
        return 'primitive_news',

    @classmethod
    def harvest(cls, specific_url_news=None, specific_url_group=None):
        if specific_url_news is None and specific_url_group is None:
            raise ValueError('Defina "specific_url_news" ou "specific_url_group"!')
        elif specific_url_news is not None and specific_url_group is not None:
            raise ValueError('Defina apenas "specific_url_news" ou apenas "specific_url_group", nunca ambos!')

        def get_new(url):
            phantom = webdriver.PhantomJS()

            phantom.get(url)

            space_title = phantom.find_element_by_class_name('materia-titulo')
            title = space_title.find_element_by_class_name('entry-title').text
            linha_fina = space_title.find_element_by_tag_name('h2').text

            if len(phantom.find_elements_by_class_name('updated')) > 0:
                date = phantom.find_element_by_class_name('updated').text
            else:
                date = phantom.find_element_by_class_name('published').text
            regexp_date = re.compile('(\d+)/(\d+)/(\d+).*')
            date_day, date_month, date_year = regexp_date.search(date).groups()

            article = '\n'.join([i.text for i in phantom.find_element_by_class_name('materia-conteudo').find_elements_by_tag_name('p')])

            primitive_id = cls.db.update_primitive_row(
                {'title': title, 'linha_fina': linha_fina, 'article': article, 'date_day': int(date_day), 'date_month': int(date_month), 'date_year': int(date_year)},
                primitive_filter={'url': url}, primitive_name='primitive_news'
            )
            cls.update_my_table({}, primitive_id=primitive_id, primitive_name='primitive_news')
            for i in phantom.find_element_by_class_name('lista-de-entidades').find_elements_by_tag_name('a'):
                cls.update_my_table({'name': i.text, 'url': i.get_attribute('href')}, primitive_id=primitive_id, primitive_name='primitive_news', table='tags')
            cls.update_crawler_status(True, primitive_id=primitive_id, primitive_name='primitive_news')

        if specific_url_news is not None:
            get_new(specific_url_news)
        else:
            phantom = webdriver.PhantomJS()

            phantom.get(specific_url_group)
            for i in phantom.find_elements_by_class_name('feed-text-wrapper'):
                get_new(i.find_element_by_class_name('feed-post-link').get_attribute('href'))
