# How to install and use PyProj / GeoPandas on Mac M1 and PyCharm

## Use miniforge
- Geopandas on Mac M1: see https://github.com/geopandas/geopandas/issues/1816#issuecomment-1003093329
- In terminal (in ARM mode, not Rosetta): 
~~~
  brew install --cask miniforge
~~~
- Add miniforge to PyCharm and install GeoPandas: https://docs.anaconda.com/anaconda/user-guide/tasks/pycharm/

## Alternative: Use Docker
- Run project in Docker container under PyCharm: https://www.jetbrains.com/help/pycharm/using-docker-as-a-remote-interpreter.html#example
- PyCharm uses /opt/project as default path mapping to project root, therefore file path of files to be used accordingly.

## Alternative: Install base libraries through Brew
Using Python 3.10.2: Install packages using command line ot within PyCharm interpreter settings: 
~~~
brew install gdal
brew install proj
pip install pyproj
~~~
If required:
~~~ 
pip install geopandas
~~~
