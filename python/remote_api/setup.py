from setuptools import find_packages, setup

setup(
    name='lynxkite-python-api',
    version='2.10.0',
    install_requires=[
        'requests',
    ],
    extras_require={
        'dev': [
            'pytest',
            'ruamel.yaml>=0.15',
            'numpy',
            'pandas',
        ]
    },
    python_requires='>=3.6',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    author='Lynx Analytics',
    author_email='lynxkite@lynxanalytics.com',
    description='Python API for LynxKite',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    url='https://lynxkite.com/',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: Other/Proprietary License',
        'Operating System :: OS Independent',
    ],
)
