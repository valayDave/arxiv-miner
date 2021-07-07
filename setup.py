from setuptools import setup, find_packages

version = '2.0.3'

with open('Readme.md', 'r') as fh:
    long_description = fh.read()

setup(name='arxiv_miner',
      version=version,
      description='ArXiv-Miner: Mine/Scrape Arxiv-Papers To Structured Datasets',
      author='Valay Dave',
      author_email='valaygaurang@gmail.com',
      license='Apache License 2.0',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='https://github.com/valayDave/arxiv-miner',
      packages=find_packages(),
      py_modules=['arxiv_miner', ],
      include_package_data=True,
      install_requires = [
        'arxiv==0.5.3',
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
      ],
      classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
      ])
