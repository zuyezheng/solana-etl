import setuptools

with open('README.md', 'r', encoding='utf-8') as fh:
    long_description = fh.read()

setuptools.setup(
    name='solana-etl',
    version='0.0.1',
    author='Zuye Zheng',
    author_email='zuyezheng@gmail.com',
    description='ETL for Solana',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/zuyezheng/solana-etl',
    project_urls={
        'Bug Tracker': 'https://github.com/zuyezheng/solana-etl/issues',
    },
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: OS Independent',
    ],

    packages=setuptools.find_packages(),
    entry_points={
        'console_scripts': [
            'solana-extract-batch = src.extract.ExtractBatch:main',
            'solana-extract-streaming = src.extract.ExtractStreaming:main',
            'solana-load-file = src.load.FileOutput:main'
        ]
    },

    python_requires='>=3.6',
    install_requires=[
        'dask==2021.12.0',
        'distributed==2021.12.0',
        'fastparquet==0.7.2',
        'neo4j==4.4.1',
        'numpy==1.22.0',
        'pandas==1.3.5',
        'solana==0.19.0'
    ]
)