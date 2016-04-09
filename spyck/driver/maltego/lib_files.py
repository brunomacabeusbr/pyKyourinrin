from lxml import etree as ET


class MaltegoEntity:
    def __init__(self, directory_save_file):
        self.directory_save_file = directory_save_file

        self.list_entity = []

    def new_entity_info_from_primitive(self, name, icon):
        self.list_entity.append(name)

        maltego_entity = ET.Element('MaltegoEntity', id='spyck.{}'.format(name), displayName=name,
                                    allowedRoot='False', largeIconResource=icon)

        properties = ET.SubElement(maltego_entity, 'Properties', value='info')
        fields = ET.SubElement(properties, 'Fields')
        ET.SubElement(fields, 'Field', name='from_primitive_id', type='string', nullable='false', hidden='true', readonly='true')
        ET.SubElement(fields, 'Field', name='from_primitive_name', type='string', nullable='false', hidden='true', readonly='true')
        ET.SubElement(fields, 'Field', name='column', type='string', nullable='false', hidden='true', readonly='true')
        ET.SubElement(fields, 'Field', name='index', type='string', nullable='false', hidden='true', readonly='true') # todo: ele é apenas da entidade info_list
        ET.SubElement(fields, 'Field', name='info', type='string', nullable='false',
                      hidden='false', readonly='true', displayName='info')

        with open(self.directory_save_file + 'spyck/spyck.{}.entity'.format(name), 'x') as f:
            f.write(ET.tostring(maltego_entity, encoding='utf8', pretty_print=True, xml_declaration=False).decode())

    def new_entity_primitive(self, name, icon, main_column_name, main_column_type):
        self.list_entity.append(name)

        maltego_entity = ET.Element('MaltegoEntity', id='spyck.{}'.format(name), displayName=name,
                                    allowedRoot='True', largeIconResource=icon)

        properties = ET.SubElement(maltego_entity, 'Properties', value=main_column_name)
        fields = ET.SubElement(properties, 'Fields')
        tag_primitive_name = ET.SubElement(fields, 'Field', name='primitive_name', type='string', nullable='false',
                                           hidden='true', readonly='true')
        ET.SubElement(tag_primitive_name, 'SampleValue').text = name
        ET.SubElement(fields, 'Field', name='table_id', type='int', nullable='false', hidden='false',
                      readonly='false', displayName='table_id')
        translate_type = {'TEXT': 'string', 'INTEGER': 'int', 'FLOAT': 'float'}
        ET.SubElement(fields, 'Field', name=main_column_name, type=translate_type[main_column_type], nullable='true',
                      hidden='false', readonly='false', displayName=main_column_name)

        with open(self.directory_save_file + 'spyck/spyck.{}.entity'.format(name), 'x') as f:
            f.write(ET.tostring(maltego_entity, encoding='utf8', pretty_print=True, xml_declaration=False).decode())

    # todo: precisa ser generalizado! pois pode ser que algum crawler populator receba mais que apenas um único crawler_param
    def new_entity_crawler_populator(self, name, icon, crawler_param):
        self.list_entity.append(name)

        maltego_entity = ET.Element('MaltegoEntity', id='spyck.{}'.format(name), displayName=name,
                                    allowedRoot='True', largeIconResource=icon)

        properties = ET.SubElement(maltego_entity, 'Properties', value=crawler_param)
        fields = ET.SubElement(properties, 'Fields')
        tag_populator_crawler = ET.SubElement(fields, 'Field', name='populator_crawler', type='string', nullable='false', hidden='true',
                                           readonly='true')
        ET.SubElement(tag_populator_crawler, 'SampleValue').text = name
        ET.SubElement(fields, 'Field', name=crawler_param, type='string', nullable='true',
                      hidden='false', readonly='false', displayName=crawler_param)

        with open(self.directory_save_file + 'spyck/spyck.{}.entity'.format(name), 'x') as f:
            f.write(ET.tostring(maltego_entity, encoding='utf8', pretty_print=True, xml_declaration=False).decode())

    def save_layer(self):
        layer_xml = ET.Element('filesystem')
        folder_root = ET.SubElement(layer_xml, 'folder', name='Maltego')

        folder_entities = ET.SubElement(folder_root, 'folder', name='Entities')
        folder_first_personal = ET.SubElement(folder_entities, 'folder', name='Personal')
        ET.SubElement(folder_first_personal, 'file', name='spyck.Unknown.entity', url='spyck.Unknown.entity')

        folder_level1entities = ET.SubElement(folder_root, 'folder', name='Level1Entities')
        folder_second_personal = ET.SubElement(folder_level1entities, 'folder', name='Personal')
        for i in self.list_entity:
            ET.SubElement(folder_second_personal, 'file', name='spyck.{}.entity'.format(i), url='spyck/spyck.{}.entity'.format(i))

        ET.SubElement(folder_root, 'folder', name='Level2Entities')

        with open(self.directory_save_file + 'layer.xml', 'x') as f:
            f.write(ET.tostring(layer_xml, encoding='utf8', xml_declaration=True, pretty_print=True, doctype='<!DOCTYPE filesystem PUBLIC "-//NetBeans//DTD Filesystem 1.2//EN" "http://www.netbeans.org/dtds/filesystem-1_2.dtd">').decode())


class MaltegoTransform:
    def __init__(self, command, working_directory, directory_save_file, debug=False):
        self.command = command
        self.working_directory = working_directory
        self.directory_save_file = directory_save_file
        self.debug = str(debug)

        self.list_transform = []

    def new_transform(self, name, entity_exclusive, parameter):
        for i in entity_exclusive: # gambiarra por conta do constraint ser um "e lógico", e não "ou lógico", e há crawlers que podem ser executador tanto por uma como por outra primitive
            # MaltegoTransform
            maltego_transform = ET.Element('MaltegoTransform', name='spyck.{}_{}'.format(name, i), displayName=name, abstract='false',
                                           visibility='public', requireDisplayInfo='false')
            ET.SubElement(maltego_transform, 'TransformAdapter').text = 'com.paterva.maltego.transform.protocol.v2.LocalTransformAdapterV2'

            input_constraints = ET.SubElement(maltego_transform, 'InputConstraints')
            ET.SubElement(input_constraints, 'Entity', type='spyck.{}'.format(i))

            with open(self.directory_save_file + '/local/spyck.{}_{}.transform'.format(name, i), 'x') as f:
                f.write(ET.tostring(maltego_transform, encoding='utf8', xml_declaration=False, pretty_print=True).decode())

            # TransformSettings
            transform_settings = ET.Element('TransformSettings', enabled='true', disclaimerAccepted='false', showHelp='true')
            properties = ET.SubElement(transform_settings, 'Properties')
            ET.SubElement(properties, 'Property', name='transform.local.command', type='string', popup='false').text = self.command
            ET.SubElement(properties, 'Property', name='transform.local.parameters', type='string', popup='false').text = 'driver/maltego/__init__.py {}'.format(parameter)
            ET.SubElement(properties, 'Property', name='transform.local.working-directory', type='string', popup='false').text = self.working_directory
            ET.SubElement(properties, 'Property', name='transform.local.debug', type='boolean', popup='false').text = self.debug

            with open(self.directory_save_file + '/local/spyck.{}_{}.transformsettings'.format(name, i), 'x') as f:
                f.write(ET.tostring(transform_settings, encoding='utf8', xml_declaration=False, pretty_print=True).decode())

            #
            self.list_transform.append(name + '_' + i)

    def save_layer(self):
        layer_xml = ET.Element('filesystem')
        folder_root = ET.SubElement(layer_xml, 'folder', name='Maltego')

        folder_first = ET.SubElement(folder_root, 'folder', name='TransformRepositories')
        folder_second = ET.SubElement(folder_first, 'folder', name='Local')
        for i in self.list_transform:
            ET.SubElement(folder_second, 'file', name='spyck.{}.transform'.format(i), url='local/spyck.{}.transform'.format(i))
            ET.SubElement(folder_second, 'file', name='spyck.{}.transformsettings'.format(i), url='local/spyck.{}.transformsettings'.format(i))

        with open(self.directory_save_file + 'layer.xml', 'x') as f:
            f.write(ET.tostring(layer_xml, encoding='utf8', xml_declaration=True, pretty_print=True, doctype='<!DOCTYPE filesystem PUBLIC "-//NetBeans//DTD Filesystem 1.2//EN" "http://www.netbeans.org/dtds/filesystem-1_2.dtd">').decode())


class MaltegoMessage:
    def __init__(self):
        self.message = ET.Element('MaltegoMessage')
        self.transform_response_message = ET.SubElement(self.message, 'MaltegoTransformResponseMessage')
        self.entities = ET.SubElement(self.transform_response_message, 'Entities')

    def add_entity(self, entity_type, value=None, weight=None, icon_url=None, display_information={}):
        class MaltegoMessageEntity:
            def __init__(self, sub_element, value=None, weight=None, icon_url=None, display_information={}):
                self.sub_element = sub_element
                self.additional_fields = ET.SubElement(self.sub_element, 'AdditionalFields')
                ET.SubElement(self.sub_element, 'Value').text = value
                ET.SubElement(self.sub_element, 'Weight').text = weight
                ET.SubElement(self.sub_element, 'IconURL').text = icon_url

                if len(display_information) > 0:
                    self.display_information = ET.SubElement(self.sub_element, 'DisplayInformation')
                    for k, v in display_information.items():
                        ET.SubElement(self.display_information, 'Label', Name=k, Type='text/html').text = v

            def add_additional_fields(self, tag, value):
                ET.SubElement(self.additional_fields, 'Field', Name=str(tag)).text = str(value)

        return MaltegoMessageEntity(ET.SubElement(self.entities, 'Entity', Type=entity_type), value=value, weight=weight, icon_url=icon_url, display_information=display_information)

    def show(self):
        print(ET.tostring(self.message, encoding='utf8', method='xml').decode())
