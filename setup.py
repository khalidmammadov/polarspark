from setuptools import setup, find_packages

setup(
    name='plspark',                    # The name of your package
    version='0.1.0',                      # Version of your package
    packages=find_packages(),             # Automatically find package directories
    install_requires=[                    # External dependencies, add more as needed
        # 'pandas>=1.1.0'
        # 'cloudpickle==3.0.0'
        # 'numpy==2.0.2'
        'polars==1.8.2'
    ],
    author='Khalid Mammadov',
    author_email='your.email@example.com',
    description='Spark on Polars',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/yourusername/my_project',  # Optional: project URL (GitHub, etc.)
    classifiers=[                         # Classifiers help users find your project
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.6',              # Specify the Python version compatibility
)