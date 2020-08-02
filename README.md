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

Todo: schema, tablename, polygon geometry fieldname and label fieldname should become command line parameters

