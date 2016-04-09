import sys

# por alguma razão, PRECISA ter o java 1.7
# sudo apt-get install oracle-java7-installer
# sudo apt-get install oracle-java7-set-default
# e se quiser trocar depois a versão
# update-alternatives --config java

# todo
#  pegar o fork do paolo https://github.com/brunomacabeusbr/casefile-extender
#  e começar a jogar as minhas coisas nele, documentar tudo em inglês, colocar manual do spyck no macalogs para depois traduzir para o inglês


# Precisamos garantir que todos os módulos do spyck serão carregados
# e não serão afetados pelo working directory em que for ser executado o script
# todo: talvez exista um método mais elegante de se fazer isso
import os

def path_parent(path):
    return os.path.abspath(os.path.join(path, os.pardir))

folder_this_file = os.path.dirname(os.path.realpath(__file__))
folder_spyck = path_parent(path_parent(folder_this_file))
sys.path.append(path_parent(folder_spyck))
sys.path.append(folder_spyck)
sys.path.append(folder_spyck + '/crawler')

#
DEBUG = False # coloque True para ver o parâmetro que o Maltego manda para cá através de um transform

import spyck.driver.maltego.lib_files

def execute_crawler():
    if 'populator_crawler' in args.keys():
        # essa esquisitice é para saber quais entity row que foram afetadas pelo crawler populator
        # para isso, adicionamos um código que será executado antes da função update_crawler_status
        list_entity_row = []
        from crawler import Crawler

        class EntityRowRecord:
            def __init__(self, f):
                self.update_crawler_status = f

            def __call__(self, *foo, **kwargs):
                list_entity_row.append((kwargs['entity_id'], kwargs['entity_name']))
                self.update_crawler_status(*foo, **kwargs)

        Crawler.update_crawler_status = EntityRowRecord(getattr(db, 'crawler_' + args['populator_crawler']).update_crawler_status)
        #

        getattr(db, 'crawler_' + args['populator_crawler']).harvest(**{k: v for k, v in args.items() if k != 'populator_crawler'})

        mm = lib_files.MaltegoMessage()
        for entity_id, entity_name in list_entity_row:
            x = mm.add_entity('spyck.{}'.format(entity_name[10:]), value=Crawler.db.execute('SELECT * FROM ' + entity_name + ' WHERE id=?', (entity_id,)).fetchone()[1])
            x.add_additional_fields('table_id', entity_id)
            x.add_additional_fields('entity_name', entity_name[10:])
        mm.show()
    else:
        crawler_name = sys.argv[2]

        getattr(db, 'crawler_' + crawler_name).harvest(**{'entity_' + args['entity_name']: int(args['table_id'])})
        get_info_all()


def get_info_all():
    infos = db.get_entity_row_info(int(args['table_id']), args['entity_name'])

    mm = lib_files.MaltegoMessage()
    for k, v in infos.items():
        if v is None:
            continue

        if k[:6] == 'entity':
            entity_name = k.split('_', 3)
            x = mm.add_entity('spyck.' + entity_name, value='{}: {} id {}'.format(entity_name[-1]. entity_name[1], v))
            x.add_additional_fields('table_id', v)
            x.add_additional_fields('entity_name', entity_name[1])
        elif type(v) is list:
            x = mm.add_entity('spyck.info_list', value='{} from {} {}: {} sub-itens'.format(k, args['entity_name'], args['table_id'], len(v)))
            x.add_additional_fields('dict_path', "['" + k + "']")
        else:
            x = mm.add_entity('spyck.info', value='{}: {}'.format(k, v))

        x.add_additional_fields('from_entity_id', args['table_id'])
        x.add_additional_fields('from_entity_name', args['entity_name'])
    mm.show()


def unpack_list():
    infos = db.get_entity_row_info(int(args['from_entity_id']), args['from_entity_name'])
    infos = eval('infos' + args['dict_path'])

    mm = lib_files.MaltegoMessage()

    if type(infos) is list:
        for i in range(len(infos)):
            x = mm.add_entity('spyck.info_list', value='sub-iten {} from {} by {} {}'.format(i, args['dict_path'], args['from_entity_name'], args['from_entity_id']))
            x.add_additional_fields('dict_path', args['dict_path'] + '[' + str(i) + ']')
            x.add_additional_fields('from_entity_id', args['from_entity_id'])
            x.add_additional_fields('from_entity_name', args['from_entity_name'])
    else:
        for k, v in infos.items():
            if v is None:
                continue

            if type(v) is list:
                x = mm.add_entity('spyck.info_list', value='{} from {} by {} {}'.format(k, args['dict_path'], args['from_entity_name'], args['from_entity_id']))
                x.add_additional_fields('dict_path', args['dict_path'] + "['" + k + "']")
            else:
                if k[:6] == 'entity':
                    entity_name = k.split('_', 3)
                    x = mm.add_entity('spyck.' + entity_name[1], value='{}: {} id {}'.format(entity_name[-1], entity_name[1], v))
                    x.add_additional_fields('table_id', v)
                    x.add_additional_fields('entity_name', entity_name[1])
                else:
                    x = mm.add_entity('spyck.info', value='{}: {}'.format(k, v))

            x.add_additional_fields('from_entity_id', args['from_entity_id'])
            x.add_additional_fields('from_entity_name', args['from_entity_name'])

    mm.show()


def generate_files():
    maltego_folder = sys.argv[2]
    import os
    import inspect
    from lxml import etree as ET
    from crawler import Crawler

    def change_jar(file_name_jar, file_name_union):
        # todo: manterá os arquivos já existentes, alterando apenas os presentes no file_name_union - esse comportamento deve ser mantido?
        from subprocess import Popen, PIPE
        p = Popen(['jar', 'uf', file_name_jar, file_name_union], stdout=PIPE, cwd=maltego_folder)
        p.stdout.read()
        assert(p.wait() == 0)

        # apagar o file_name_union, pois ele agora é desnecessário
        import shutil
        shutil.rmtree(maltego_folder + file_name_union)

    ###
    # read xml config
    maltego_config = ET.parse(folder_this_file + '/config.xml').getroot()
    entity_icon_dict = {i.attrib['name']: i.text for i in maltego_config.find('entity_icons').findall('entity')}

    ###
    # entidades
    entities_names = []

    dir_save_files = maltego_folder + '/com/paterva/maltego/entities/common/'
    os.makedirs(dir_save_files)

    if not os.path.exists(dir_save_files + 'spyck'):
        os.makedirs(dir_save_files + 'spyck')

    me = lib_files.MaltegoEntity(dir_save_files)

    # arbitrárias
    me.new_entity_info_from_entity('info', 'Phrase')
    me.new_entity_info_from_entity('info_list', 'OsiModelGolden')

    # crawlers populares
    crawler_populator = []
    for current_crawler in Crawler.__subclasses__():
        jump = False
        harvest_args = inspect.getargspec(current_crawler.harvest_debug).args
        if len(harvest_args) == 1:
            continue
        for i in harvest_args:
            if i[:7] == 'entity_':
                jump = True
                break
        if jump:
            continue

        me.new_entity_crawler_populator(current_crawler.name(), 'Objects', harvest_args[1]) # todo: precisa ser generalizado! pois pode ser que algum crawler populator receba mais que apenas um único crawler_param
        crawler_populator.append(current_crawler)

    # entities
    for current_xml in os.listdir(folder_spyck + '/entities/'):
        crawler_root = ET.parse(folder_spyck + '/entities/' + current_xml).getroot()
        current_xml = current_xml[:-4]
        entities_names.append(current_xml)

        column_name, column_type = crawler_root.find('column').find('name').text, crawler_root.find('column').find('type').text

        me.new_entity_entity(current_xml, entity_icon_dict[current_xml], column_name, column_type)

    # layer
    me.save_layer()

    # colocar os arquivos no .jar do Maltego
    change_jar('com-paterva-maltego-entities-common.jar', 'com')

    ###
    # crawlers
    dir_save_files = maltego_folder + '/com/paterva/maltego/transforms/standard/'
    os.makedirs(dir_save_files)
    os.makedirs(dir_save_files + '/local')

    mt = lib_files.MaltegoTransform('/usr/bin/python3', folder_spyck, dir_save_files)

    # crawler arbitrário get_info_all
    mt.new_transform('get_info_all', [i for i in entities_names], 'get_info_all')
    mt.new_transform('unpack_list', ['info_list'], 'unpack_list')

    # crawlers de verdade
    for current_crawler in Crawler.__subclasses__():
        if current_crawler in crawler_populator:
            continue

        mt.new_transform(current_crawler.name(), [i[10:] for i in inspect.getargspec(current_crawler.harvest_debug).args if i[:7] == 'entity_'], 'execute_crawler {}'.format(current_crawler.name()))

    # crawler populator
    for current_crawler in crawler_populator:
        mt.new_transform(current_crawler.name(), [current_crawler.name()], 'execute_crawler {}'.format(current_crawler.name()))

    # layer
    mt.save_layer()

    # colocar os arquivos no .jar do Maltego
    change_jar('com-paterva-maltego-transforms-standard.jar', 'com')


def parse_arguments(argv):
    values = {}

    for i in argv.split('#'):
        vars_values = i.split('=')
        values[vars_values[0]] = vars_values[1]

        if i[:11] == 'properties.':
            values['entity'] = i[11:].split('=')[0]

    return values

if sys.argv[1] == 'generate_files':
    generate_files()
else:
    if DEBUG:
        # coloque True caso queira debugar e ver a mensagem
        mm = lib_files.MaltegoMessage()
        mm.add_entity('spyck.info_list', value=' '.join(sys.argv[1:]))
        mm.add_entity('spyck.info', value='#'.join(['{}={}'.format(k, v) for k, v in parse_arguments(sys.argv[-1]).items()]))
        mm.show()
        exit()

    from spyck.database import ManagerDatabase
    db = ManagerDatabase(trigger=False)

    args = None
    if sys.argv[1] == 'execute_crawler':
        args = parse_arguments(sys.argv[-1])
        execute_crawler()
    elif sys.argv[1] == 'get_info_all':
        args = parse_arguments(sys.argv[-1])
        get_info_all()
    elif sys.argv[1] == 'unpack_list':
        args = parse_arguments(sys.argv[-1])
        unpack_list()

exit()
