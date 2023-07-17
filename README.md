# matlab-database-toolbox
A high-level toolbox for using the MongoDB database in MATLAB.

If it was useful for you don't forget the stars for the project

This toolbox is useful for machine learning  and data-engineering projects.

Postgres database manager will be added to the toolbox.

## Important Notes:
     The user-password option for the Mongoc++ interface is different from those that are expected such as Python.
     We provided a Python code ("add_user_mongodb.py") that resolves the User-Pass problem with MATLAB MongoDB.
     If you have a MongoDB with a User-Pass, just run "demo_password.m" one time and then start your get or insert actions.
     

## Dependency:
     Run on MATLAB 2021 or newer
     
## Usage:
     You can check the MongoDB class functions by running the demo_password.m, demo_simple.m and demo_pro.m files.
