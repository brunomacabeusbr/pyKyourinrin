import sys
import os
from collections import OrderedDict

# Load xml
my_path = os.path.dirname(os.path.realpath(__file__))
xml_name = sys.argv[1]
xml_file = my_path + '/../crawler_xml/' + xml_name + '.xml'
crawler_file = my_path + '/../crawler/' + xml_name + '.py'

# Read xml
import xml.etree.ElementTree as ET

xml_root = ET.parse(xml_file).getroot()

primitive_required = [(i.attrib['type_requirement'], 'primitive_' + i.text) for i in xml_root.find('primitive_required').findall('primitive')]
table_main_columns = [(i.find('name').text, i.find('type')) for i in xml_root.find('database').find('table_main').findall('column')]
tables_secondary = OrderedDict(
    (k.find('name').text, [(i.find('name').text, i.find('type')) for i in k.findall('column')])
    for k in xml_root.find('database').findall('table_secondary')
)
reference_list = {i.find('name').text: i.find('reference').text for i in xml_root.find('database').findall('table_secondary') if i.find('reference') is not None}
macro_at_data_root = xml_root.find('database').find('macro_at_data')
macro_at_data = []
if macro_at_data_root:
    macro_at_data = [i.text for i in macro_at_data_root.findall('name')]
dependencies = [[i2.text for i2 in i.findall('dependence')] for i in xml_root.find('dependencies').findall('route')]
crop = [i.text for i in xml_root.find('crop').findall('info')]
harvest = {i.tag: i.text for i in xml_root.find('harvest') if i.tag == 'url'} # todo: colocar para usar a tag url
harvest['param_additional'] = [i.text for i in xml_root.find('harvest').findall('param_additional')]

# Verificar nomes
# explicação do black_list: no primeiro elemento da tupla, fica o nome que não pode ser usado, no segundo a sua localização
#   os parâmetros de localização são: 'first' se o nome não pode ficar no começo ou 'equal' se o nome não pdoe ser exatamente aquele
black_list_name_crawler = [('main', 'first'), ('primitive', 'first')]
black_list_name_column = [('primitive', 'first'), ('reference', 'first'), ('id', 'equal')]
white_list_type_column = ['TEXT', 'INTEGER', 'FLOAT']

def check_black_list(name, black_list):
    for i in black_list:
        if i[1] == 'first':
            if name[:len(i[0])] == i[0]:
                raise ValueError('O nome "{}" não pode ser usado, pois começa com "{}", que é usado pelo pyKyourinrin'.format(name, i[0]))
        elif i[1] == 'equal':
            if name == i[0]:
                raise ValueError('O nome "{}" não pode ser usado, pois é uma palavra reservada'.format(name))

def check_white_list(name, white_list):
    if name not in white_list:
        raise ValueError('O nome "{}" não pode ser usado, pois os únicos válidos nesse lugar são {}'.format(name, ', '.join(white_list)))

check_black_list(xml_name, black_list_name_crawler)

for i in table_main_columns:
    check_black_list(i[0], black_list_name_column)
    if 'primitive' not in i[1].attrib:
        check_white_list(i[1].text, white_list_type_column)

for i in tables_secondary.values():
    for i2 in i:
        check_black_list(i2[0], black_list_name_column)
        if 'primitive' not in i2[1].attrib:
            check_white_list(i2[1].text, white_list_type_column)

for i in macro_at_data:
    check_black_list(i, black_list_name_column)

# Write py
crawler_name_camel_case = ''.join(i.title() for i in xml_name.split('_'))

def columns_of_table(list_columns):
    list_column_name_and_type = []
    list_foreigns = []

    for i in list_columns:
        if type(i[1]) == str:
            list_column_name_and_type.append(
                (i[0],
                 i[1])
            )
            continue

        if 'primitive' in i[1].attrib:
            column_name = 'primitive_{}_id_{}'.format(i[1].text, i[0])

            list_column_name_and_type.append(
                (
                    column_name,
                    'INTEGER'
                )
            )

            list_foreigns.append(('FOREIGN KEY({})'.format(column_name), 'REFERENCES primitive_{}(id)'.format(i[1].text)))
        else:
            list_column_name_and_type.append(
                (
                    i[0],
                    i[1].text
                )
            )

    return ",'\n".join(
        "                            '" + i[0] + ' ' + i[1]
        for i in [
            (i2[1] + '_id', 'INTEGER') for i2 in primitive_required if i2[0] == 'harvest' or i2[0] == 'write'
        ] + list_column_name_and_type + list_foreigns
    )

def write_tables_secondary():
    if len(tables_secondary) == 0:
        return ''

    # sql
    to_return = '\n\n'
    for i in tables_secondary.keys():
        reference_mod_string = ''
        tables_secondary[i] = tables_secondary[i]
        to_return += "        self.db.execute('CREATE TABLE IF NOT EXISTS %s('\n"
        if i in reference_list.values():
            tables_secondary[i].insert(0, ('reference', 'INTEGER PRIMARY KEY AUTOINCREMENT'))
        if i in reference_list.keys():
            tables_secondary[i].insert(0, ('reference_' + reference_list[i], 'INTEGER'))
            tables_secondary[i].append(('FOREIGN', 'KEY(reference_' + reference_list[i] + ') REFERENCES %s(reference_' + reference_list[i] + ')'))
            reference_mod_string += ", self.name() + '_{}'".format(reference_list[i])
        to_return += columns_of_table(tables_secondary[i]) + "'"
        to_return += "\n                        ');' % (self.name() + '_" + i + "'" + reference_mod_string + "))\n\n"

    # read_my_secondary_tables
    def add_key_reference(from_table):
        if from_table not in reference_list.keys():
            return ''
        else:
            predecessor_reference = []
            def make_predecessor_reference(info):
                predecessor_reference.append(info)

                if info not in reference_list.keys():
                    return
                else:
                    make_predecessor_reference(reference_list[info])

            make_predecessor_reference(from_table)
            predecessor_reference = predecessor_reference[1:]
            predecessor_reference = predecessor_reference[::-1]
            return ", 'reference': ({})".format(inter_to_tuple(predecessor_reference))

    to_return += '    @staticmethod\n    def read_my_secondary_tables():\n        return (\n'
    to_return += inter_to_tuple_multi_line(["{'table': '" + i + "'" + add_key_reference(i) + "}" for i in tables_secondary.keys()], 3)
    to_return += '\n        )'

    # return
    return to_return

def write_macro_at_data():
    if len(macro_at_data) == 0:
        return ''

    to_return = '\n    @staticmethod\n    def macro_at_data():\n'

    # functions
    for i in macro_at_data:
        to_return += '        def {}(read):\n            pass # todo\n\n'.format(i)

    # write return
    to_return += '        return (\n'
    to_return += inter_to_tuple_multi_line(["{'column_name': '" + i + "', 'how': " + i + "}" for i in macro_at_data], 3)
    to_return += '\n        )\n'

    #
    return to_return

def inter_to_tuple(inter):
    return (', '.join("'{}'".format(i) for i in inter), "'" + inter[0] + "',")[len(inter) == 1]

def inter_to_tuple_multi_line(inter, indentation_ideep):
    return (',\n'.join('    ' * indentation_ideep + '{}'.format(i) for i in inter), '    ' * indentation_ideep + inter[0] + ',')[len(inter) == 1]

def write_crop():
    if len(crop) == 1:
        return "'{}',".format(crop)
    else:
        return ', '.join(["'{}'".format(i) for i in crop])

def write_dependencies():
    if len(dependencies[0]) == 0:
        return "'',"
    elif len(dependencies) == 1:
        return inter_to_tuple(dependencies[0])
    else:
        return ', '.join('(' + inter_to_tuple(i) + ')' for i in dependencies)

def harvest_params():
    params = []

    # primitives
    primitive_harvest = [i[1] for i in primitive_required if i[0] == 'harvest']
    if len(primitive_harvest):
        params.extend(primitive_harvest)
        params.append('dependencies')

    # additional
    params.extend(harvest['param_additional'])

    # return
    params_code = ', '.join(i + '=None' for i in params)
    if len(params_code) != 0:
        return ', ' + params_code
    else:
        return ''

with open(crawler_file, 'x') as f:
    content = \
"""from . import Crawler


class Crawler""" + crawler_name_camel_case + """(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
""" + columns_of_table(table_main_columns) + """'
                        ');' % self.name())""" + write_tables_secondary() + """
""" + write_macro_at_data() + """
    @staticmethod
    def name():
        return '""" + xml_name + """'

    @staticmethod
    def dependencies():
        return """ + write_dependencies() + """

    @staticmethod
    def crop():
        return """ + write_crop() + """

    @staticmethod
    def primitive_required():
        return """ + inter_to_tuple([i[1] for i in primitive_required]) + """

    @classmethod
    def harvest(cls""" + harvest_params() + """):
        # todo: make it

"""
    f.write(content)
