from setuptools import setup, find_packages

setup(
    name='cc_dynamodb3',
    packages=find_packages(),
    install_requires=[
        'bunch>=1.0.1',
        'boto3>=1.2.2',
        'schematics==1.1.1',
    ],
    dependency_links=[
        'git+https://github.com/clearcare/cc_hcl_translator.git@0.1.0#egg=hcl_translator',
    ],
    tests_require=['pytest', 'mock', 'factory_boy', 'moto'],
    version='1.0.4',
    description='A dynamodb common configuration abstraction',
    author='Paul Craciunoiu',
    author_email='pcraciunoiu@clearcareonline.com',
    url='https://github.com/clearcare/cc_dynamodb3',
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: OS Independent',
        'Development Status :: 1 - Planning',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Topic :: System :: Distributed Computing',
    ]
)
