
Arctic fork to resolve dependencies turned to separate project

## Quickstart

### Install Argus

``
pip install git+https://github.com/man-group/argus.git
``

### Run a MongoDB

``
mongod --dbpath <path/to/db_directory>
``

## Using VersionStore
``
from argus import Argus
import quandl
``
### Connect to Local MONGODB
``
store = Argus('localhost')
``

### Create the library - defaults to VersionStore
``
store.initialize_library('NASDAQ')
``

### Access the library
``
library = store['NASDAQ']
``

### Load some data - maybe from Quandl
``
aapl = quandl.get("WIKI/AAPL", authtoken="your token here")
``

### Store the data in the library
``
library.write('AAPL', aapl, metadata={'source': 'Quandl'})
``

### Reading the data
``
item = library.read('AAPL')
aapl = item.data
metadata = item.metadata
``
