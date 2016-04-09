from . import Crawler
from aylienapiclient import textapi
import re


class CrawlerAylienConcept(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_news_id INTEGER'
                        ');' % self.name())

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_news_id INTEGER,'
                            'reference INTEGER PRIMARY KEY AUTOINCREMENT,'
                            'name TEXT,'
                            'name_url TEXT'
                        ');' % (self.name() + '_mentioned'))

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_news_id INTEGER,'
                            'reference_mentioned INTEGER,'
                            'word_type TEXT,'
                            'word_type_url TEXT,'
                            'FOREIGN KEY(reference_mentioned) REFERENCES %s(reference_mentioned)'
                        ');' % (self.name() + '_mentioned_types', self.name() + '_mentioned'))

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_news_id INTEGER,'
                            'reference_mentioned INTEGER,'
                            'name_at_article TEXT,'
                            'FOREIGN KEY(reference_mentioned) REFERENCES %s(reference_mentioned)'
                        ');' % (self.name() + '_mentioned_surface_article', self.name() + '_mentioned'))

    @staticmethod
    def read_my_secondary_tables():
        return (
            {'table': 'mentioned'},
            {'table': 'mentioned_types', 'reference': ('mentioned',)},
            {'table': 'mentioned_surface_article', 'reference': ('mentioned',)}
        )

    @staticmethod
    def name():
        return 'aylien_concept'

    @staticmethod
    def dependencies():
        return 'article',

    @staticmethod
    def crop():
        return 'concept_quotes',

    @staticmethod
    def primitive_required():
        return 'primitive_news',

    @classmethod
    def harvest(cls, primitive_news=None, dependencies=None):
        # http://docs.aylien.com/docs/concepts

        client = textapi.Client('71085b2c', '9a69067df3c7f538060fedac9c1adbc0')
        concepts = client.Concepts({'text': dependencies['article'], 'language': 'pt'})

        regexp_get_concept_name = re.compile(r'.*/(.*)$')

        cls.update_my_table({})

        for k, v in concepts['concepts'].items():
            cls.update_my_table({'name': regexp_get_concept_name.search(k).group(1).replace('_', ' '), 'name_url': k}, table='mentioned')
            reference_mentioned = cls.db.lastrowid()

            if v['types'][0] != '':
                for i in v['types']:
                    cls.update_my_table({'word_type': regexp_get_concept_name.search(i).group(1).replace('_', ' '), 'word_type_url': i, 'reference_mentioned': reference_mentioned}, table='mentioned_types')

            for i in v['surfaceForms']:
                cls.update_my_table({'name_at_article': i['string'], 'reference_mentioned': reference_mentioned}, table='mentioned_surface_article')

        cls.update_crawler_status(True)
