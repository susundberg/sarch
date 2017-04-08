__author__  = 'Pauli Salmenrinne'

from setuptools import setup



requires = [
]

setup( name='sarch',
      version="1.0.0",
      description='Simple archiving solution',
      
      scripts=['bin/sarch'],
      packages=['sarch'],
      
      long_description=open('README.rst').read(),
      url='https://github.com/susundberg/',
      author='Pauli Salmenrinne',
      author_email='susundberg@gmail.com',
      license='MIT',

      install_requires=requires,
      test_suite="test",
      classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: POSIX',
        'Programming Language :: Python :: 3.5',
        'Topic :: System :: Archiving',
        'Topic :: System :: Filesystems'
      ],
      zip_safe=True )