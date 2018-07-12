### Please have Python packages numpy and pandas installed.

Method 1:
please download the 64bit Python 2.7 version of anaconda
https://www.continuum.io/downloads

Method 2:
1: download and install the latest Python 2.7.*
2: sudo easy_install pip
3: pip install numpy
4: pip install pandas


### Go to PythonAPI folder and install the package with the following command:

To install for the first time:
    python setup.py install

To update the existing package:
    python setup.py install --force


### To import DolphinDB Python API and establish a connection to a DolphinDB server running on port 8848:

    import dolphindb as ddb
    conn=ddb.session()
	conn.connect( 'localhost', 8848)


For more examples, please refer to:

http://www.dolphindb.com/help/PythonAPI.html