from . import Crawler
import re
import tools.pdf


class CrawlerBepidResultado(Crawler):
    def create_my_table(self):
        self.db.execute('CREATE TABLE IF NOT EXISTS %s('
                            'primitive_peoples_id INTEGER,'
                            'bepid_position INTEGER,'
                            'bepid_score INTEGER,'
                            'bepid_ranked_first INTEGER,'
                            'bepid_ranked_second INTEGER'
                        ');' % self.name())

    @staticmethod
    def name():
        return 'bepid_resultado'

    @staticmethod
    def dependencies():
        return '',

    @staticmethod
    def crop():
        return 'name', 'birthday_day', 'birthday_month', 'birthday_year',

    @staticmethod
    def primitive_required():
        return 'primitive_peoples',

    @classmethod
    def harvest(cls):
        # Aviso: Antes de colher aqui, deve-se user ManagerDatabase().crawler_qselecao.harvest(specifc_concurso=2890)

        # First stage
        content_first = tools.pdf.pypdf_extract_text_from_url('http://www.bepid.ifce.edu.br/resultado_prova_selecao.pdf')
        content_first = ' '.join(content_first.replace('\xa0', ' ').strip().split())

        regexp_first = re.compile(r'(\d+)(\D+) (\d{2})\/(\d{2})\/(\d{4})(\d+,\d)(\w+)')

        # Second stage
        pdf_text_second_stage = tools.pdf.pdfminer_extract_text_from_url('http://www.bepid.ifce.edu.br/bepid_selecionados_2016.pdf')
        pdf_text_second_stage = pdf_text_second_stage.replace('\t', ' ')

        regexp_second = re.compile(r'\d\s\s(.*)')

        ranked_second_stage = regexp_second.findall(pdf_text_second_stage)[:37]
        ranked_second_stage = [i.strip() for i in ranked_second_stage]

        # Save
        for i in regexp_first.findall(content_first):
            primitive_id = cls.db.get_primitive_id_by_filter({'name': i[1], 'birthday_day': i[2], 'birthday_month': i[3], 'birthday_year': i[4]}, 'primitive_peoples')

            cls.update_my_table({'bepid_position': i[0], 'bepid_score': float(i[5].replace(',', '.')),
                                 'bepid_ranked_first': (0, 1)[i[6] == 'Classificado'],
                                 'bepid_ranked_second': (0, 1)[i[1] in ranked_second_stage]},
                                primitive_id=primitive_id, primitive_name='primitive_peoples')
            cls.update_crawler(1, primitive_id=primitive_id, primitive_name='primitive_peoples')
