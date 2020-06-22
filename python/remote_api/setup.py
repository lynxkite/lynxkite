from setuptools import find_namespace_packages, setup
import os

setup(
    name='lynxkite-python-api',
    version=os.environ.get('VERSION', 'snapshot'),
    install_requires=[
        'requests',
        'pandas',
    ],
    extras_require={
        'dev': [
            'pytest',
            'ruamel.yaml>=0.15',
            'mypy',
        ]
    },
    python_requires='>=3.6',
    packages=find_namespace_packages('src'),
    package_dir={'': 'src'},
    author='Lynx Analytics',
    author_email='lynxkite@lynxkite.com',
    description='Python API for LynxKite',
    long_description=open('README.md', 'r').read(),
    long_description_content_type='text/markdown',
    url='https://lynxkite.com/',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: GNU Affero General Public License v3 or later (AGPLv3+)',
        'Operating System :: OS Independent',
    ],
)
