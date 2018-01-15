import os
from distutils.core import setup


with open(os.path.join(os.path.dirname(__file__), 'README.md')) as fh:
    readme = fh.read()


setup(
    name='miniredis',
    version=__import__('miniredis').__version__,
    description='miniredis',
    long_description=readme,
    author='Charles Leifer',
    author_email='coleifer@gmail.com',
    url='http://github.com/coleifer/miniredis/',
    packages=[],
    py_modules=['miniredis'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
    ],
    scripts=['miniredis.py'],
    test_suite='tests')
