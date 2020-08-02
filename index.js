let polylabel = require('@mapbox/polylabel');
let netherlands = require("./netherlands.json");
let dbconfig = require("./config.json");
let pgPromise = require("pg-promise");
let QueryStream = require('pg-query-stream');
let through = require('through');
let sqlLargestPart = require("./sqllargestpart.js");
let fs = require("fs");
let proj4 = require("proj4");

const pgp = pgPromise({
    schema: dbconfig.schema
})
const db = pgp(dbconfig.connection);
console.log(`dbhost: ${dbconfig.connection.host}, dbname: ${dbconfig.connection.database}`);

proj4.defs([
    ["EPSG:4326",'+title=WGS 84 (long/lat) +proj=longlat +ellps=WGS84 +datum=WGS84 +units=degrees'],
    ["EPSG:28992", "+proj=sterea +lat_0=52.15616055555555 +lon_0=5.38763888888889 +k=0.9999079 +x_0=155000 +y_0=463000 +ellps=bessel +towgs84=565.417,50.3319,465.552,-0.398957,0.343988,-1.8774,4.0725 +units=m +no_defs"],
    ["EPSG:3857","+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +wktext  +no_defs"]
]);

let worldMercatorToGPS = proj4("EPSG:3857", "EPSG:4326");

let transformCount = 0;
let transformer = through(function write(doc){
    if (typeof doc === 'string') {
        this.queue(doc);
        return;
    }
    let feature = 
    {
        "type": "Feature",
        "properties": {
            id: doc.id
        },
        "geometry": {
            "type": "Point",
            "coordinates": worldMercatorToGPS.forward(polylabel(JSON.parse(doc.geojson).coordinates))
        }
    };
    this.queue(`${transformCount?',':'{"type":"FeatureCollection","features":['}${JSON.stringify(feature)}`);
    transformCount++;
}) 

function getGeoJSON(filename) {
    let rawdata = fs.readFileSync(filename);
    return JSON.parse(rawdata.toString('utf8'));
}
async function uploadGeoJSONTable(filename, schemaname, tablename, idField, labelPointField) {
    let geojson = getGeoJSON(filename);
    if (geojson.features.length) {
        sql = `drop table if exists "${schemaname}"."${tablename}_${labelPointField}"`;
        await db.none(sql);
        sql = `create table "${schemaname}"."${tablename}_${labelPointField}" ("${idField}" int, "${labelPointField}" geometry(Point,4326))`;
        await db.none(sql);
        let batchsize = 1000; // uses batches of values to improve insert performance
        for (let i = 0; i < geojson.features.length; i += batchsize) {
            let values = []
            for (let j = 0; j < batchsize && (i+j < geojson.features.length); j++) {
                let feature = geojson.features[i+j];
                values.push(`(${feature.properties.id}, st_setsrid(st_geomfromgeojson('${JSON.stringify(feature.geometry)}'),4326))`)
            }
            sql = `insert into "${schemaname}"."${tablename}_${labelPointField}" ("${idField}", "${labelPointField}") values ${values.join(',')}`;
            try {
                await db.none(sql);
            } catch(err) {
                console.log(`${err.message}, sql=${sql}`);
            }
        }
        sql = `alter table "${schemaname}"."${tablename}" add column "${labelPointField}" geometry(Point,4326)`;
        try {
            await db.none(sql);
        } catch (err) {
            // ignore, 'add colum if not exists' seems buggy?'
        }
        sql = `drop index if exists "${schemaname}"."${tablename}_${schemaname}"`;
        await db.none(sql);
        sql = `update "${schemaname}"."${tablename}" set "${labelPointField}"=l."${labelPointField}" from "${schemaname}"."${tablename}_${labelPointField}" l where "${schemaname}"."${tablename}"."${idField}"=l.${idField}`
        await db.none(sql);
        sql = `create index if not exists "${tablename}_${labelPointField}" on "${schemaname}"."${tablename}" using gist("${labelPointField}");`
        await db.none(sql);
    }
}


async function addOrUpdateLabelPoint(schemaname, tablename, idField, polygonField, labelPointField) {
    try {
        let sql = `select "${idField}" as id, st_asgeojson(st_transform(LargestPart("${polygonField}"),3857)) as geojson from "${schemaname}"."${tablename}"`;
        const qs = new QueryStream(sql);
        let writeStream = fs.createWriteStream(`${__dirname}/output.geojson`);
        let writeStreamClosed = new Promise((resolve,reject)=>{
            writeStream.on('close', ()=>{
                resolve()
            });
            writeStream.on('error', reject);
        })
        transformCount = 0;
        await db.stream(qs, stream =>{
            stream.on('end', ()=>{
                transformer.end(']}');
            });
            stream.pipe(transformer, {end: false}).pipe(writeStream);
        });
        await writeStreamClosed;
        await uploadGeoJSONTable(`${__dirname}/output.geojson`, schemaname, tablename, idField, labelPointField);
    } catch (error) {
        console.warn(`addOrUpdateLabelPoint failed: ${error.message}`);
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
    let tablename = "gt_gemeenteperceelsectie";
    let idField = "id";
    let polygonField = "geom";
    let labelPointfield = "labelpoint";
    await addOrUpdateLabelPoint(schemaname, tablename, idField, polygonField, labelPointfield);
    process.exit(0);
})();

//let labelpoint = polylabel(netherlands.features[0].geometry.coordinates);
//console.log(labelpoint);