# How to install and use GeoPandas on Mac M1 and PyCharm
- Geopandas on Mac M1: see https://github.com/geopandas/geopandas/issues/1816#issuecomment-1003093329
- In terminal (in ARM mode, not Rosetta): 
~~~
  brew install --cask miniforge
~~~
- Add miniforge to PyCharm and install GeoPandas: https://docs.anaconda.com/anaconda/user-guide/tasks/pycharm/
- Or even better run project in Docker container under PyCharm: https://www.jetbrains.com/help/pycharm/using-docker-as-a-remote-interpreter.html#example
- PyCharm uses /opt/project as default path mapping to project root, tehrefore file path of files to be used accordingly.
