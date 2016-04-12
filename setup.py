from setuptools import setup

setup(
    name='spyck',
    version='0.0.2',
    description='Crawler framework',
    author='Bruno Macabeus',
    url='http://macalogs.com.br/spyck-apresentacao/',
    download_url='https://github.com/zetaresearch/spyck',
    keywords=['crawler', 'framework'],
    install_requires=[
        'requests',
        'PyPDF2',
        'selenium',
        'pyslibtesseract',
        'aylien-apiclient'
        # todo: precisa exigir o phantomjs
    ],
    packages=['spyck'],
    packages_data={'spyck': ['crawler_xml/etufor.xml']}
)
