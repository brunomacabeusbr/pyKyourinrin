from setuptools import setup

setup(
    name='pyKyorinrin',
    install_requires=[
        'requests',
        'PyPDF2',
        'selenium',
        'networkx',
        'matplotlib'
        # todo: precisa exigir o phantomjs
    ],
    packages=['pyKyorinrin']
)