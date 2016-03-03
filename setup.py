from setuptools import setup

setup(
    name='pyKyorinrin',
    install_requires=[
        'requests',
        'PyPDF2',
        'selenium',
        'networkx',
        'matplotlib',
        'pyslibtesseract',
        'aylien-apiclient'
        # todo: precisa exigir o phantomjs
    ],
    packages=['pyKyorinrin']
)