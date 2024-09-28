from setuptools import setup, find_packages

setup(
    name='plspark',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pandas>=1.4.4',
        'numpy>=1.21',
        'polars==1.8.2',
        'pyarrow>=4.0.0',
        'tzlocal==5.2'
    ],
    author='Khalid Mammadov',
    author_email='your.email@example.com',
    description='Spark on Polars',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/khalidmammadov/plspark',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',              # Specify the Python version compatibility
)