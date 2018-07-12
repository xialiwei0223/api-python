# DolphinDB Python API


### Environment setup

Before Running the scripts,please have python packages numpy and pandas installed.

Method 1:
please download the 64bit python 2.7 version of anaconda, which contains python and all packages we need to run xxdb api python scripts
https://www.continuum.io/downloads


Method 2:
1: download and install the latest python 2.7.*
2: sudo easy_install pip
3: pip install numpy
4: pip install pandas


Then, please go to our python api folder and install the package through the following command:

To install for the first time:
    python setup.py install

To update the existing install:
    python setup.py install --force


### Get it started

Assuming you have dolphindb SERVER running on port 8848, you can run the following commands in python console

```
    import dolphindb as ddb
    
    #start a DolphinDB session
    s = ddb.session() 
    
    # connect to DolphinDB server
    success = s.connect('localhost', 8848)
    
    # or connect with login info
    s = ddb.session() 
    success = s.connect('localhost', 8848, "admin","123456") 
    
    if success:
        obj = s.run("Your XXDB script") #run dolphinDB script
        print obj
```

For more examples, please refer to

http://www.dolphindb.com/help/PythonAPI.html

###  Package introduction

1: session.py 
DolphinDB api for querying DolphinDB server

2: settings.py
contain the data form and type definitions, and other settings

3:data_factory.py
python data building factory based on returned DolphinDB object

4: socket_util.py
python socket protocols

5: date_util.py
converting integer into python datetime object

6: table.py
manipulate DolphinDB SQL query through python class "Table".
