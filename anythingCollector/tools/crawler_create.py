import sys
import os

my_path = os.path.dirname(os.path.realpath(__file__))
crawler_folder = my_path + '/../crawler/'
crawler_name_array = sys.argv[1:]
crawler_name_underscore = '_'.join(crawler_name_array)
crawler_name_camel_case = ''.join(i.title() for i in crawler_name_array)

with open(crawler_folder + crawler_name_underscore + '.py', 'x') as f:
    content = \
"""from . import Crawler


class Crawler""" + crawler_name_camel_case + """(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'peopleid INTEGER,'
                            # todo: make it
                            'FOREIGN KEY(peopleid) REFERENCES peoples(id)'
                        ');' % self.name())

    @staticmethod
    def name():
        return '""" + crawler_name_underscore + """'

    @staticmethod
    def dependencies():
        return '',  # todo: make it

    @staticmethod
    def crop():
        return '',  # todo: make it

    @classmethod
    def harvest(cls, id=None, dependencies=None):
        # todo: make it

"""
    f.write(content)
