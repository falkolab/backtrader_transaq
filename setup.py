from setuptools import find_packages, setup


setup(
    name='backtrader_transaq',
    packages=find_packages(),
    version='0.1.1',
    description='Transaq connector integration for backtrader',
    author='Andrey Tkachenko',
    author_email='falko.lab@gmail.com',
    license='MIT',
    install_requires=['transaqpy', 'backtrader', 'pytz', 'tzlocal'],
    setup_requires=['pytest-runner', 'grpcio-tools'],
    tests_require=['pytest'],
    test_suite='tests',
)
