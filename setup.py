from setuptools import setup
from setuptools import find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'google-api-python-client>=1.5.1',
    'oauth2client>=2.0.1,<3.0',
    'humanize>=0.5.1',
    'unicodecsv>=0.14.1',
    'pytz',
    'tzlocal',
    'tabulate',
    'python-dateutil'
]

setup(
    name='gwrappy',
    version='0.4.3',
    description="User friendly wrapper for Google APIs",
    long_description=readme + '\n\n' + history,
    author="Daniel Poon",
    author_email='daniel.poon.wenjie@gmail.com',
    url='https://github.com/danielpoonwj/gwrappy',
    packages=find_packages(),
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords=['google', 'cloud', 'gcloud'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5'
    ]
)
