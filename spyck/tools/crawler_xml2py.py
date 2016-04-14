import sys
import os
import xml.etree.ElementTree as ET

# Load xml
my_path = os.path.dirname(os.path.realpath(__file__))
xml_name = sys.argv[1]
xml_file = my_path + '/../crawler_xml/' + xml_name + '.xml'
crawler_file = my_path + '/../crawler/foo.py' #crawler_file = my_path + '/../crawler/' + xml_name + '.py'

crawler_name_camel_case = ''.join(i.title() for i in xml_name.split('_'))

xml_root = ET.parse(xml_file).getroot()

# Write py
def iter_to_tuple(inter, external_parentheses=True):
    return (
        ('', '(')[external_parentheses] + ', '.join("'{}'".format(i) for i in inter) + ('', ')')[external_parentheses],
        ('', '(')[external_parentheses] + "'" + inter[0] + "'," + ('', ')')[external_parentheses]
    )[len(inter) == 1]


def iter_to_tuple_multi_line(inter):
    return '(\n' + ',\n'.join('            ' + '{}'.format(i) for i in inter) + ',\n        )'


def list_entity_of_require(require_type):
    return [i.text for i in xml_root.find('entity_required').findall('entity') if i.attrib['type_requirement'] in require_type]


def write_create_my_table():
    def xml_tree_to_crete_table_sql(xml_tree):
        start_command = "self.db.execute('CREATE TABLE IF NOT EXISTS %s('"

        columns = ''
        foreign_key = ''
        references_tables = []

        # adicionar colunas dos id das entidades
        for i in list_entity_of_require(['harvest', 'write']):
            columns += "\n                            'entity_{}_id INTEGER,'".format(i)

        # adicionar coluna "reference", se necessário
        if xml_tree.tag == 'table_secondary' and \
                len([i for i in xml_root.find('database').findall('table_secondary') if i.findtext('reference') == xml_tree.findtext('name')]):
            columns += "\n                            'reference INTEGER PRIMARY KEY AUTOINCREMENT,'"

        # adicionar coluna "reference_##table-name", caso a tabela seja de referência
        if len(xml_tree.findall('reference')) > 0:
            for i in xml_tree.findall('reference'):
                columns += "\n                            'reference_{} INTEGER,'".format(i.text)
                references_tables.append(i.text)

        # adicionar colunas dos dados do crawler
        for i in xml_tree.findall('column'):
            if i.find('entity') is None:
                columns += "\n                            '{} {},'".format(i.find('name').text, i.find('type').text) # testar usar o findtext
            else:
                column_name = 'entity_{}_id_{}'.format(i.find('entity').text, i.find('name').text)
                columns += "\n                            '{} INTEGER,'".format(column_name)
                foreign_key += "\n                            'FOREIGN KEY({}) REFERENCES entity_{}(id),'".format(column_name, i.find('entity').text)

        # adicionar foreign key para as colunas "reference_##table-name", casa haja alguma
        for i in references_tables:
            columns += "\n                            'FOREIGN KEY(reference_{}) REFERENCES %s(reference_{}),'".format(i, i)

        # parâmetros para formatar a string
        params_string_format =\
            ['self.name(){}'.format((lambda: " + '_{}'".format(xml_tree.findtext('name')) if xml_tree.find('name') is not None else '')())] +\
            ["self.name() + '_{}'".format(i) for i in references_tables]

        if len(params_string_format) == 1:
            end_command = "');' % {})".format(params_string_format[0])
        else:
            end_command = "');' % ({}))".format(', '.join(params_string_format))

        #
        return '        {}{}{}\n                        {}'.format(start_command, columns, foreign_key, end_command)

    return '\n\n'.join(
        [xml_tree_to_crete_table_sql(xml_root.find('database').find('table_main'))] +
        [xml_tree_to_crete_table_sql(i) for i in xml_root.find('database').findall('table_secondary')]
    )


def write_read_my_secondary_tables():
    def list_references(from_table):
        # essa função recebe como parâmetro um <table_secondary>
        # e retornará uma string do código da tupla com a lista de referências feita pela tabela
        references_list = []

        current_table_xml = from_table
        while current_table_xml.find('reference') is not None:
            current_reference = current_table_xml.findtext('reference')
            references_list.append(current_reference)

            for i in xml_root.find('database').findall('table_secondary'):
                if i.findtext('name') == current_reference:
                    current_table_xml = i
                    break

        return iter_to_tuple(references_list[::-1])

    return '        return ' + iter_to_tuple_multi_line(
        [
            "{{'table': '{}'{}}}".format(
                i.findtext('name'),
                (lambda: ", 'reference': {}".format(list_references(i)) if i.find('reference') is not None else '')()
            ) for i in xml_root.find('database').findall('table_secondary')
        ]
    )


def write_macro_at_data():
    code_functions = '\n\n'.join(
        ['        def {}(read):\n           pass # todo'.format(i.text) for i in xml_root.find('database').find('macro_at_data').findall('name')]
    )

    code_return = '        return ' + iter_to_tuple_multi_line(
        [
            "{{'column_name': '{}', 'how': {}}}".format(i.text, i.text) for i in xml_root.find('database').find('macro_at_data').findall('name')
        ]
    )

    return code_functions + '\n\n' + code_return


def write_dependencies():
    if xml_root.find('dependencies').find('route') is None:
        return 'None'
    else:
        if len(xml_root.find('dependencies').findall('route')) == 1:
            return iter_to_tuple([i.text for i in xml_root.find('dependencies').find('route').findall('dependence')], external_parentheses=False)
        else:
            return ', '.join(
                [
                    iter_to_tuple(
                        [i2.text for i2 in i.findall('dependence')]
                    ) for i in xml_root.find('dependencies').findall('route')
                ]
            )


def write_harvest():
    params = ['cls']

    # parâmetros das entidades
    if len(list_entity_of_require(['harvest'])) > 0:
        for i in list_entity_of_require(['harvest']):
            params.append('entity_{}=None'.format(i))
        params.append('dependencies=None')

    # parâmetros adicionais
    for i in xml_root.find('harvest').findall('param_additional'):
        default_value = 'None'
        if 'default_type' in i.attrib:
            if i.attrib['default_type'] == 'int':
                default_value = int(i.attrib['default_value'])
            elif i.attrib['default_type'] == 'str':
                default_value = "'{}'".format(i.attrib['default_value'])

        params.append('{}={}'.format(i.text, default_value))

    #
    return ', '.join(params)


with open(crawler_file, 'x') as f:
    f.write(
'from . import Crawler\n' +
'\n' +
'\n' +
'class Crawler{}(Crawler):\n'.format(crawler_name_camel_case) +
'    def create_my_table(self):\n' +
'{}'.format(write_create_my_table()) +
(
    lambda:
    '\n' +
    '\n' +
    '    @staticmethod\n' +
    '    def read_my_secondary_tables():\n' +
    '{}'.format(write_read_my_secondary_tables())
    if xml_root.find('database').find('table_secondary') is not None
    else ''
)() +
(
    lambda:
    '\n' +
    '\n' +
    '    @staticmethod\n' +
    '    def macro_at_data():\n' +
    '{}'.format(write_macro_at_data())
    if xml_root.find('database').find('macro_at_data') is not None
    else ''
)() +
'\n' +
'\n' +
'    @staticmethod\n' +
'    def name():\n' +
"        return '{}'\n".format(xml_name) +
'\n' +
'    @staticmethod\n' +
'    def dependencies():\n' +
'        return {}\n'.format(write_dependencies()) +
'\n' +
'    @staticmethod\n' +
'    def crop():\n' +
'        return {}\n'.format(iter_to_tuple([i.text for i in xml_root.find('crop').findall('info')], external_parentheses=False)) +
'\n' +
'    @staticmethod\n' +
'    def entity_required():\n' +
'        return {}\n'.format(iter_to_tuple(['entity_' + i for i in list_entity_of_require(['harvest', 'write', 'reference'])], external_parentheses=False)) + # todo: talvez seja bom simplificar nos crawler para precisar apenas do nome da entidade
'\n' +
'    @classmethod\n' +
'    def harvest({}):\n'. format(write_harvest()) +
'        # todo'
    )
