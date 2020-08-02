module.exports = `
create or replace function LargestPart (g geometry)
    returns geometry
    language plpgsql immutable as
$func$
begin
    -- Non-multi geometries can just pass through
    if GeometryType(g) in ('POINT', 'LINESTRING', 'POLYGON') then
        return g;
    -- MultiPolygons and GeometryCollections that contain Polygons
    elsif not ST_IsEmpty(ST_CollectionExtract(g, 3)) then
        return (
            select geom
            from (
                select (ST_Dump(ST_CollectionExtract(g,3))).geom
            ) as dump
            order by ST_Area(geom) desc
            limit 1
        );
    -- MultiLinestrings and GeometryCollections that contain Linestrings
    elsif not ST_IsEmpty(ST_CollectionExtract(g, 2)) then
        return (
            select geom
            from (
                select (ST_Dump(ST_CollectionExtract(g,2))).geom
            ) as dump
            order by ST_Length(geom) desc
            limit 1
        );
    -- Other geometry types are not really handled but we at least try to
    -- not return a MultiGeometry.
    else
        return ST_GeometryN(g, 1);
    end if;
end;
$func$;
`