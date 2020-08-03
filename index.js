let polylabel = require('@mapbox/polylabel');
let dbconfig = require("./config.json");
let pgPromise = require("pg-promise");
let QueryStream = require('pg-query-stream');
let sqlLargestPart = require("./sqllargestpart.js");
let proj4 = require("proj4");
const BatchStream = require('batched-stream');
let { Writable } = require('stream');

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

    sql = `drop table if exists "${schemaname}"."${tempTableName}"`;
    await db.none(sql);
    sql = `create table "${schemaname}"."${tempTableName}" ("${idField}" int, "${labelPointField}" geometry(Point,4326))`;
    await db.none(sql);  
    
    const recordMapper = (record) => {
        let result = {};
        result[idField] = record[idField];
        let inputGeometry = JSON.parse(record.geojson);
        let outputGeometry = {type:"Point"};
        outputGeometry.coordinates = worldMercatorToGPS.forward(polylabel(inputGeometry.coordinates));
        result[labelPointField] = `st_setsrid(st_geomfromgeojson('${JSON.stringify(outputGeometry)}'),4326)`;
        return result;
    }

    await new Promise((resolve,reject)=>{
        let sql = `select "${idField}" as id, st_asgeojson(st_transform(LargestPart("${polygonField}"),3857)) as geojson from "${schemaname}"."${tablename}"`;
        const queryStream = new QueryStream(sql);

        const batch = new BatchStream({size : batchsize, objectMode: true, strictMode: false});

        let insertDatabase = new Writable({
            objectMode:true, 
            write(records, encoding, callback){
                (async ()=>{
                    try {
                        records = records.map(record=>recordMapper(record));
                        //let sql = pgp.helpers.insert(records, columnSet);
                        let sql = `insert into "${schemaname}"."${tempTableName}" ("${idField}","${labelPointField}") values ${records.map(record=>`(${record.id},${record[labelPointField]})`).join(',')}`
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
                resolve()}
            )
            .on('error', (error)=>{
                console.log(`error ${error.message}`);
                reject(error)
            })
        });
    });
    sql = `alter table "${schemaname}"."${tablename}" add column "${labelPointField}" geometry(Point,4326)`;
    try {
        await db.none(sql);
    } catch (err) {
        // ignore, 'add column if not exists' seems buggy?'
    }
    sql = `drop index if exists "${schemaname}"."${tablename}_${labelPointField}"`;
    await db.none(sql);
    sql = `update "${schemaname}"."${tablename}" set "${labelPointField}"=l."${labelPointField}" from "${schemaname}"."${tempTableName}" l where "${schemaname}"."${tablename}"."${idField}"=l.${idField}`
    await db.none(sql);
    sql = `create index if not exists "${tablename}_${labelPointField}" on "${schemaname}"."${tablename}" using gist("${labelPointField}");`
    await db.none(sql);
    sql = `drop table if exists "${schemaname}"."${tempTableName}"`;
    await db.none(sql);
}

(async ()=> {
    try {
        let result = await db.none(sqlLargestPart);
    } catch (err) {
        console.error(err.message);
        process.exit(1);
    }
    let schemaname = "geotag";
    let tablename = "gt_buurt";
    let idField = "id";
    let polygonField = "geom";
    let labelPointfield = "labelpoint";
    await addOrUpdateLabelPoint(schemaname, tablename, idField, polygonField, labelPointfield);
    process.exit(0);
})();
