from setuptools import setup

setup(
    name='skope_loader',
    version='0.0.1',
    packages=['skope_loader'],
    zip_safe=True,

    install_requires=[
        "elasticsearch>=6.0.0,<7.0.0",
        "awesome-slugify>=1.6.0,<2.0.0",
        "furl",
        "geojson",
        "nodeenv",
        "PyYAML",
        "requests",
    ],

    entry_points={
        'console_scripts': [
            'dsloader=skope_loader.dsloader:main',
            'dsindex=skope_loader.dsindex:main',
        ]
    },
)
        
