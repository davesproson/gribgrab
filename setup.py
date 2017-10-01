from setuptools import setup

setup(
    name='gribgrab',
    version='0.1.0',
    description='Dowload GFS/GDAS data from Nomads',
    author='Dave Sproson',
    author_email='davesproson@gmail.com',
    packages=['gribgrab'],
    install_requires=['requests']
)
