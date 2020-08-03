let dbconfig = require("./config.json");
let pgPromise = require("pg-promise");
let QueryStream = require('pg-query-stream');
let sqlLargestPart = require("./sqllargestpart.js");
let proj4 = require("proj4");
const BatchStream = require('batched-stream');
let { Writable } = require('stream');

let polylabel = require('@mapbox/polylabel');

const pgp = pgPromise({
    schema: dbconfig.schema
})
const db = pgp(dbconfig.connection);
console.log(`dbhost: ${dbconfig.connection.host}, dbname: ${dbconfig.connection.database}`);

proj4.defs([
    ["EPSG:4326",'+title=WGS 84 (long/lat) +proj=longlat +ellps=WGS84 +datum=WGS84 +units=degrees'],
    ["EPSG:3857","+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs"]
]);

let worldMercatorToGPS = proj4("EPSG:3857", "EPSG:4326");

async function addOrUpdateLabelPoint(schemaname, tablename, idField, polygonField, labelPointField, tempTableName) {
    const batchsize = 1000;
    if (!tempTableName) {
        tempTableName = `tmp_${tablename}_${labelPointField}`
    }
    let sqlParams = {schemaname:schemaname, tablename:tablename, idField:idField, polygonField:polygonField, labelPointField:labelPointField, tempTableName:tempTableName}
    let labelPointError;
    try {
        let sql = `select pg_typeof($(idField:name)) from $(schemaname:name).$(tablename:name) limit 1`;
        sqlParams.idFieldType = (await db.one(sql, sqlParams)).pg_typeof;
        sql = `drop table if exists $(schemaname:name).$(tempTableName:name)`;
        await db.none(sql, sqlParams);
        sql = `create table $(schemaname:name).$(tempTableName:name) ($(idField:name) $(idFieldType:raw), $(labelPointField:name) geometry(Point,4326))`;
        await db.none(sql, sqlParams);  
        
        const recordMapper = (record) => {
            let result = {};
            result[idField] = record[idField];
            let inputGeometry = JSON.parse(record.geojson);
            let outputGeometry = {type:"Point"};
            outputGeometry.coordinates = worldMercatorToGPS.forward(polylabel(inputGeometry.coordinates));
            result[labelPointField] = `st_setsrid(st_geomfromgeojson('${JSON.stringify(outputGeometry)}'),4326)`;
            return result;
        }

        escapeName = (name) => name.toString().replace(/"/g, '""');

        await new Promise((resolve,reject)=>{
            let sql = `select "${escapeName(idField)}" as id, st_asgeojson(st_transform(LargestPart("${escapeName(polygonField)}"),3857)) as geojson from "${escapeName(schemaname)}"."${escapeName(tablename)}"`;
            const queryStream = new QueryStream(sql);

            const batch = new BatchStream({size : batchsize, objectMode: true, strictMode: false});

            let insertDatabase = new Writable({
                objectMode:true, 
                write(records, encoding, callback){
                    (async ()=>{
                        try {
                            records = records.map(record=>recordMapper(record));
                            let sql = pgp.helpers.insert(records, new pgp.helpers.ColumnSet([
                                {name: idField},
                                {name: labelPointField, mod: ":raw"}
                            ], {table: {table: tempTableName, schema: schemaname}}));
                            await db.none(sql);
                        } catch(err) {
                            return callback(err);
                        }
                        callback();
                    })();
                }
            });
            
            db.stream(queryStream, stream=>{
                stream.pipe(batch)
                    .pipe(insertDatabase)
                    .on('finish', ()=>{
                        resolve()
                    })
                    .on('error', (error)=>{
                        reject(error);
                    });
            }).catch(error=>{
                reject(new Error(`Stream error: ${error.message}`));
            });
        });
        sql = 'alter table $(schemaname:name).$(tablename:name) add column if not exists $(labelPointField:name) geometry(Point,4326)';
        await db.none(sql, sqlParams);
        sqlParams.indexName = `${tablename}_${labelPointField}`
        sql = `drop index if exists $(schemaname:name).$(indexName:name)`;
        await db.none(sql, sqlParams);
        sql = `update $(schemaname:name).$(tablename:name) set $(labelPointField:name)=l.$(labelPointField:name) from $(schemaname:name).$(tempTableName:name) l where $(schemaname:name).$(tablename:name).$(idField:name)=l.$(idField:name)`
        await db.none(sql,sqlParams);
        sql = `create index if not exists $(indexName:name) on $(schemaname:name).$(tablename:name) using gist($(labelPointField:name));`
        await db.none(sql, sqlParams);
        sql = `drop table if exists $(schemaname:name).$(tempTableName:name)`;
        await db.none(sql, sqlParams);
    } catch(error) {
        labelPointError = new Error(`labelPoint error: ${error.message}`);
    } finally {
        try {
            let sql = `drop table if exists $(schemaname:name).$(tempTableName:name)`;
            await db.none(sql, sqlParams);
        } catch (error) {

        }
        if (labelPointError) {
            throw labelPointError;
        }
    }
}

(async ()=> {
    try {
        let result = await db.none(sqlLargestPart);
    } catch (err) {
        console.error(err.message);
        process.exit(1);
    }
    let schemaname = "geotag";
    let tablename = "gt_buurtlw";
    let idField = "id";
    let polygonField = "geom";
    let labelPointfield = "labelpoint";
    try{
        await addOrUpdateLabelPoint(schemaname, tablename, idField, polygonField, labelPointfield);
    } catch (error) {
        console.log(`update Error ${error.message}`);
    }
    process.exit(0);
})();
