// Dataset 1
CREATE INDEX vendor_id_index FOR (v:Vendor) ON (v.vendor_id);
CREATE INDEX location_lat_index FOR (l:Location) ON (l.latitude);
CREATE INDEX location_long_index FOR (l:Location) ON (l.longitude);
LOAD CSV WITH HEADERS FROM 'file:///bog_clean.csv' AS row
MERGE (vendor:Vendor {vendor_id: row.vendor_id})
MERGE (pickup:Location {latitude: toFloat(row.pickup_latitude), longitude: toFloat(row.pickup_longitude)})
MERGE (dropoff:Location {latitude: toFloat(row.dropoff_latitude), longitude: toFloat(row.dropoff_longitude)})
CREATE (trip:Trip {
  id: toInteger(row.id),
  pickup_datetime: row.pickup_datetime,
  dropoff_datetime: row.dropoff_datetime,
  trip_duration: toInteger(row.trip_duration),
  dist_meters: toInteger(row.dist_meters),
  wait_sec: toInteger(row.wait_sec),
  source: 'BOG'  // Added property to tag the source of the data
})
MERGE (trip)-[:PICKED_UP_FROM]->(pickup)
MERGE (trip)-[:DROPPED_OFF_AT]->(dropoff)
MERGE (trip)-[:PROVIDED_BY]->(vendor);

// Dataset 2

LOAD CSV WITH HEADERS FROM 'file:///mex_clean.csv' AS row
MERGE (vendor:Vendor {vendor_id: row.vendor_id})
MERGE (pickup:Location {latitude: toFloat(row.pickup_latitude), longitude: toFloat(row.pickup_longitude)})
MERGE (dropoff:Location {latitude: toFloat(row.dropoff_latitude), longitude: toFloat(row.dropoff_longitude)})
CREATE (trip:Trip {
  id: toInteger(row.id),
  pickup_datetime: row.pickup_datetime,
  dropoff_datetime: row.dropoff_datetime,
  trip_duration: toInteger(row.trip_duration),
  dist_meters: toInteger(row.dist_meters),
  wait_sec: toInteger(row.wait_sec),
  source: 'mex' // Added property to tag the source of the data
})
MERGE (trip)-[:PICKED_UP_FROM]->(pickup)
MERGE (trip)-[:DROPPED_OFF_AT]->(dropoff)
MERGE (trip)-[:PROVIDED_BY]->(vendor);

// Dataset 3
LOAD CSV WITH HEADERS FROM 'file:///uio_clean.csv' AS row
MERGE (vendor:Vendor {vendor_id: row.vendor_id})
MERGE (pickup:Location {latitude: toFloat(row.pickup_latitude), longitude: toFloat(row.pickup_longitude)})
MERGE (dropoff:Location {latitude: toFloat(row.dropoff_latitude), longitude: toFloat(row.dropoff_longitude)})
CREATE (trip:Trip {
  id: toInteger(row.id),
  pickup_datetime: row.pickup_datetime,
  dropoff_datetime: row.dropoff_datetime,
  trip_duration: toInteger(row.trip_duration),
  dist_meters: toInteger(row.dist_meters),
  wait_sec: toInteger(row.wait_sec),
  source: 'UIO'  // Added property to tag the source of the data
})
MERGE (trip)-[:PICKED_UP_FROM]->(pickup)
MERGE (trip)-[:DROPPED_OFF_AT]->(dropoff)
MERGE (trip)-[:PROVIDED_BY]->(vendor);

// Dataset 4
LOAD CSV WITH HEADERS FROM 'file:///all-data_clean.csv' AS row
MERGE (vendor:Vendor {vendor_id: row.vendor_id})
MERGE (pickup:Location {latitude: toFloat(row.pickup_latitude), longitude: toFloat(row.pickup_longitude)})
MERGE (dropoff:Location {latitude: toFloat(row.dropoff_latitude), longitude: toFloat(row.dropoff_longitude)})
CREATE (trip:Trip {
  id: toInteger(row.id),
  pickup_datetime: row.pickup_datetime,
  dropoff_datetime: row.dropoff_datetime,
  trip_duration: toInteger(row.trip_duration),
  dist_meters: toInteger(row.dist_meters),
  wait_sec: toInteger(row.wait_sec),
  source: 'ALL-DATA'  // Added property to tag the source of the data
})
MERGE (trip)-[:PICKED_UP_FROM]->(pickup)
MERGE (trip)-[:DROPPED_OFF_AT]->(dropoff)
MERGE (trip)-[:PROVIDED_BY]->(vendor);

// Query 1 Find the average trip duration and distance for each type of vendor.
MATCH (t:Trip)-[:PROVIDED_BY]->(v:Vendor)
WHERE t.source = 'ALL-DATA'
WITH v.vendor_id AS Vendor, avg(t.trip_duration) AS AverageDuration, avg(t.dist_meters) AS AverageDistance
RETURN Vendor, AverageDuration, AverageDistance;

// Query 2 List all trips longer than 2 hours and sort them by distance in descending order.
MATCH (t:Trip)
WHERE t.source = 'ALL-DATE' AND t.trip_duration > 7200  // 2 hours = 7200 seconds
RETURN t.id AS TripID, t.trip_duration AS TripDuration, t.dist_meters AS Distance
ORDER BY t.dist_meters DESC;

// Query 3 Identify trips where the waiting time is more than 20% of the total trip duration.
MATCH (t:Trip)
WHERE t.source = 'ALL-DATE' AND t.wait_sec > 0.2 * t.trip_duration
RETURN t.id AS TripID, t.wait_sec AS WaitingTime, t.trip_duration AS TotalDuration;

// Query 4 For a given pickup location [0.2, 0.2], find all trips within a 10 km radiu
MATCH (t:Trip)-[:PICKED_UP_FROM]->(p:Location)
WHERE t.source = 'ALL-DATE' AND point.distance(point({latitude: p.latitude, longitude: p.longitude}), point({latitude: 0.2, longitude: 0.2})) <= 10000
RETURN t.id AS TripID, p.latitude AS PickupLatitude, p.longitude AS PickupLongitude
ORDER BY point.distance(point({latitude: p.latitude, longitude: p.longitude}), point({latitude: 0.2, longitude: 0.2})) ASC;

// Query 5 Identify trips for the same vendor where the pickup times occur within a short timeframe (e.g., within 30 minutes of each other)

MATCH (trip1:Trip)-[:PROVIDED_BY]->(vendor:Vendor)
WHERE trip1.source = 'UIO'
WITH vendor, trip1
ORDER BY trip1.pickup_datetime
WITH vendor, collect(trip1) AS trips
UNWIND range(0, size(trips)-2) AS i
WITH vendor, trips[i] AS trip1, trips[i+1..] AS laterTrips
UNWIND laterTrips AS trip2
WITH trip1, trip2
WHERE trip2.pickup_datetime <= trip1.pickup_datetime + duration('PT30M')
RETURN trip1.id AS TripID_a, trip1.pickup_datetime AS PickupTime_a, trip2.id AS TripID_b, trip2.pickup_datetime AS PickupTime_b
ORDER BY trip1.pickup_datetime;
