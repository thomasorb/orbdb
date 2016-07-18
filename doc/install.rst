==============
 Installation
==============

Install MySQL
=============

To install MySQL::

  sudo apt-get install libmysqld-dev mysql-server
  adduser --system --group --home /var/lib/mysql mysql

You should also check the permissions and ownership of the /var/lib/mysql directory::
  /var/lib/mysql: drwxr-xr-x   mysql    mysql

Python package::
  sudo pip install MySQL-python

To check::

  python
  >> import MySQLdb

Create mysql user and SITELLE database::

  mysql -u root -p -h localhost
  mysql> CREATE USER 'orbdb'@'localhost' IDENTIFIED BY 'orbdb-passwd';
  mysql> CREATE DATABASE sitelle;
  mysql> GRANT ALL PRIVILEGES ON sitelle.* TO 'orbdb'@'localhost';
  

Database password:

root password is 'sitelle'
