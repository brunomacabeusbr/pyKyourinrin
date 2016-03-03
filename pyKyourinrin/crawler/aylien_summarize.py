from . import Crawler
from aylienapiclient import textapi


class CrawlerAylienSummarize(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_news_id INTEGER'
                        ');' % self.name())

        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_news_id INTEGER,'
                            'text TEXT'
                        ');' % (self.name() + '_sentences'))

    @staticmethod
    def read_my_secondary_tables():
        return (
            {'table': 'sentences'},
        )

    @staticmethod
    def column_export():
        def key_sentences(read):
            if len(read['sentences']) > 0:
                return read['sentences']
            else:
                return None

        return (
            {'column_name': 'key_sentences', 'how': key_sentences},
        )

    @staticmethod
    def name():
        return 'aylien_summarize'

    @staticmethod
    def dependencies():
        return 'title', 'article'

    @staticmethod
    def crop():
        return 'key_sentences',

    @staticmethod
    def primitive_required():
        return 'primitive_news',

    @classmethod
    def harvest(cls, primitive_news=None, dependencies=None, total_sentences=5):
        # http://docs.aylien.com/docs/summarize

        if primitive_news is None:
            raise ValueError('Você obrigatorialmente deve fornecer um id de primitive news!')

        client = textapi.Client('71085b2c', '9a69067df3c7f538060fedac9c1adbc0')
        summary = client.Summarize({'title': dependencies['title'], 'text': dependencies['article'], 'sentences_number': total_sentences})

        cls.update_my_table({})
        for i in summary['sentences']:
            cls.update_my_table({'text': i}, table='sentences')
        cls.update_crawler_status(True)
