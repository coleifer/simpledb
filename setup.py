import os
from setuptools import setup, find_packages


with open(os.path.join(os.path.dirname(__file__), 'README.md')) as fh:
    readme = fh.read()


setup(
    name='simpledb',
    version=__import__('simpledb').__version__,
    description='simpledb',
    long_description=readme,
    author='Charles Leifer',
    author_email='coleifer@gmail.com',
    url='http://github.com/coleifer/simple-db/',
    packages=find_packages(),
    extras_require=extras_require,
    package_data={
        'simpledb': [
        ],
    },
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    test_suite='runtests.runtests',
    entry_points={
        'console_scripts': [
            'simpledb = simpledb:main'
        ]
    },
    scripts=['simpledb.py'],
)
