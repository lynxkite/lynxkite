from setuptools import find_namespace_packages, setup

setup(
    name='lynx-automation',
    version=os.environ.get('VERSION', 'snapshot'),
    install_requires=[
        'lynxkite-python-api',
        'flask-appbuilder==1.12',
        'apache-airflow[mysql]==1.10.1',
        'tzlocal==1.5.1',
        'pendulum==1.4.4',
    ],
    extras_require={
        'dev': [
            'pytest',
            'mypy',
        ]
    },
    python_requires='>=3.6',
    packages=find_namespace_packages('src'),
    package_dir={'': 'src'},
)
