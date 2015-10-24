from setuptools import setup, find_packages
import ez_setup
ez_setup.use_setuptools()
setup(
    name = "SolrClient",
    version = "0.0.4",
    author = "Nick Vasilyev",
    author_email = "nick.vasilyev1@gmail.com",
    packages = find_packages(),
    include_package_data = True,
    url = "https://github.com/moonlitesolutions/SolrClient",
    description = "Python based client for Solr. ",
    install_requires = [
        "requests>=2.2.1","psutil"
    ],
)