from distutils.core import setup

setup(
    # Application name:
    name="SolrClient",

    # Version number (initial):
    version="0.1.0",

    # Application author details:
    author="Nick Vasilyev",
    author_email="nick.vasilyev1@gmail.com",

    # Packages
    packages=["SolrClient"],

    # Include additional files into the package
    include_package_data=True,

    # Details
    url="http://solrclient.readthedocs.org/en/latest/",

    #
    # license="LICENSE.txt",
    description="Python based client for Solr. ",

    # long_description=open("README.txt").read(),

    # Dependent packages (distributions)
    install_requires=[
        "requests",
    ],
)