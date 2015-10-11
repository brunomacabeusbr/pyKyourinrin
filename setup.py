from setuptools import setup

setup(
    name='anythingCollector',
    install_requires=[
        'requests',
        'PyPDF2',
        'selenium',
        'networkx',
        'matplotlib'
        # todo: precisa exigir o phantomjs
    ],
    packages=['anythingCollector']
)