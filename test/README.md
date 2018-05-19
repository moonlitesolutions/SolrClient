# Testing Info

Please include unit tests for all new functionality and bug fixes. Due to various bugs in the code and in Solr
I chose to test against a real Solr instance instead of mocking one. I like to test with multiple version of Solr as well.

If you want to test your contribution do the following:
1. Download the [SolrVagrant](https://github.com/moonlitesolutions/SolrVagrant) project
2. Edit the `Vagrantfile`, find this line and replace it with your branch:

	ansible.raw_arguments  = [
      		'--extra-vars "git_repo=https://github.com/nickvasilyev/SolrClient.git"'
    	]

2. run `vagrant up` to kick off the installation / configuration. It will take a while.
3. run `vagrant ssh` to get into the box
4. The code from the git repo will be in ~/code/
5. run `tox`


## Additional Testing Info
tox is configured to run tests on python 3.2, 3.4 and 3.5, it runs the tests by executing run_tests.sh.

run_tests.sh sets the proper environmental variables and executes all the tests one by one for each listed Solr Version.

To run a single test run something like:

./run_tests.py -py 3.5 -solr 6.3.0 -test test_client.ClientTestIndexing.test_down_solr_exception
or
./run_tests.py -py 3.5 -solr 6.3.0 -test test_client

Make sure the version of solr / python you are specified is included in the setup. 

## To Add a Solr Version
1. Update ansible/plays/playbook.yml and add the solr version
2. Create schema.xml in resources/$Version
3. Update run_tests.sh to include the version so it gets included in the tests
