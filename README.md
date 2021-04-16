# Polylabel
An early attempt to externally calculate but efficiently add label points to polygons stored in PostGIS.   
Possibly usable as skeleton for future external PostGIS record by record calculations.

### Requirements
* git
* npm / node
* PostGIS database

### Installation
```
git clone this_repository
cd this_respository
npm install
# now configure the database connection
# .. see Configuration below
# then run 
npm start
```

### Configuration
The database connection parameters should be defined in file ```config.json```.   
Copy file ```config.json.example``` to ```config.json``` and update the database connection parameters.

### usage
```
# get usage info
node index.js

usage:
node index.js table=tablename
optional parameters: schema=public, labelPointField=labelpoint, polygonField=geom, idField=id, tempTableName=null
See config.json for database connection parameters
```


