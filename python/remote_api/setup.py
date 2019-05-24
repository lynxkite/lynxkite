from setuptools import find_packages, setup

setup(
    name='lynx-remote-api',
    version='2.10.0',
    install_requires=[
        'PyYAML',
        'requests',
        'croniter',
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
    zip_safe=False,
)
