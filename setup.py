from setuptools import setup, find_packages

setup(
    name='polarspark',
    version='0.1.10',
    packages=find_packages(),
    install_requires=[
        'pandas>=2.2.3',
        'numpy>=1.21',
        'polars==1.33.1',
        'pyarrow>=4.0.0',
        'tzlocal==5.2',
        'deltalake==0.22.3',
        'watchdog',
        'sqlglot'
    ],
    author='Khalid Mammadov',
    author_email='xmamedov@gmail.com',
    description='Spark on Polars',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/khalidmammadov/polarspark',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.9',
)

