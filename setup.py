from setuptools import setup, find_packages

version = '2.0.0'

setup(name='arxiv_miner',
      version=version,
      description='ArXiv-Miner: Mine/Scrape Arxiv-Papers To Structured Datasets',
      author='Valay Dave',
      author_email='valaygaurang@gmail.com',
      license='Apache License 2.0',
      packages=find_packages(),
      py_modules=['arxiv_miner', ],
      include_package_data=True,
      install_requires = [
        'arxiv',
        'tex2py',
        'matplotlib',
        'pandas',
        'click',
        'numpy',
        'dateparser',
        'expiringdict',
        'elasticsearch',
        'elasticsearch_dsl',
        'luqum',
      ])
