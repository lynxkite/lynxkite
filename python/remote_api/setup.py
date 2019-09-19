from setuptools import find_namespace_packages, setup
from lynx_versioning import version

setup(
    name='lynxkite-fiber-python-api',
    version=version(),
    install_requires=[
        'requests',
        'pandas',
        'grpcio-tools',
        'pyarrow',
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
