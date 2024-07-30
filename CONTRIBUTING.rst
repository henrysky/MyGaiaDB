Contributor and Issue Reporting guide
=====================================

When contributing to this repository, please first discuss the big changes you wish to make via opening issue,
email, or any other method with the maintainers of this repository.

Submitting bug reports and feature requests
---------------------------------------------

Bug reports and feature requests should be submitted by creating an issue on https://github.com/henrysky/MyGaiaDB/issues

When submitting a bug report, please include a short summary of the bug, the expected behaviour, and the actual behaviour.

Pull Request
-------------

This is a general guideline to make pull request (PR).

#. Go to https://github.com/henrysky/MyGaiaDB/, click the ``Fork`` button.
#. Download your own ``MyGaiaDB`` fork to your computer by ``$git clone https://github.com/{your_username}/MyGaiaDB``.
#. Create a new branch with a short simple name that represents the change you want to make.
#. Make commits locally in that new branch, and push to your own ``MyGaiaDB`` fork.
#. Create a pull request by clicking the ``New pull request`` button.

New Databases Proposal guide
-----------------------------
If you need a new database locally and want to add such database which should be publicly avaliable to ``MyGaiaDB`` 
so other users can also use it, so you are welcome to do so.

To add a new database:

#. Add your dataset (i.e. the raw data file) download logics in ``./src/mygaiadb/data/download.py``. You should also add download logics for test suits to run such that the test suits can be ran on a small portion of the data to sanity check (you can check how it is done for existing datasets)
#. Add your new SQL schema as a ``.sql`` file under ``./src/mygaiadb/data/sql_schema/`` directory.
#. Add compilation logics (i.e. convert raw data to a SQL database) in ``./src/mygaiadb/data/compile.py``.
#. Add tests in the test suits to check the correctness of the compilation logics as well as do a simple query to make sure everything is good.