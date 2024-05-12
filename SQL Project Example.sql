-- Q1: Designing Schemas
create table if not exists scooter(
    scooter_id smallint,
    status enum('online', 'offline', 'lost/stolen') NOT NULL DEFAULT 'offline',
    primary key(scooter_id)
);

create table if not exists customer(
    user_id mediumint,
    ccnum char(16),
    expdate timestamp,
    email varchar(100) NOT NULL,
    primary key(user_id)
);

create table if not exists trip(
    trip_id int,
    user_id mediumint NOT NULL,
    scooter_id smallint NOT NULL,
    start_time timestamp NOT NULL,
    end_time timestamp,
    pickup_latitude float NOT NULL,
    pickup_longtitude float NOT NULL,
    dropoff_latitude float,
    dropoff_longtitude float,
    primary key(trip_id),
    foreign key (user_id) references customer(user_id),
    foreign key (scooter_id) references scooter(scooter_id)
);

create table if not exists scooter(
    scooter_id smallint,
    status enum('online', 'offline', 'lost/stolen') NOT NULL DEFAULT 'offline',
    primary key(scooter_id)
);

create table if not exists customer(
    user_id mediumint,
    ccnum char(16),
    expdate timestamp,
    email varchar(100) NOT NULL,
    primary key(user_id)
);

create table if not exists trip(
    trip_id int,
    user_id mediumint NOT NULL,
    scooter_id smallint NOT NULL,
    start_time timestamp NOT NULL,
    end_time timestamp,
    pickup_latitude float NOT NULL,
    pickup_longtitude float NOT NULL,
    dropoff_latitude float,
    dropoff_longtitude float,
    primary key(trip_id),
    foreign key (user_id) references customer(user_id),
    foreign key (scooter_id) references scooter(scooter_id)
);

-- Q2
/* For this part, we will use data in the mgmtmsa402 database on the class EC2 instance. 
Los Angeles International Airport (LAX/KLAX) is often a tourist's first impression of Los Angeles, and a place where 
Professor has spent way too much time. The airport has eight numbered terminals T1 thru T8 
as well as Tom Bradley International Terminal containing a main building denoted TBIT and a series of distant gates 
denoted TBIT WEST. There are also two remote facilities named IMPERIAL and MISC. The airport serves Domestic and International flights.

The City of Los Angeles maintains a ton of data about the airport including flight operations, passenger throughput,
concessions, parking availability, roadway circulation and even the location of defibrillators. For this set of exercises, we wil use 
a table called lax_pax (for "LAX passengers") which contains information about the number of passengers departing
from and arriving into different terminals in the airport on domestic and international flights. Use the following schema
*/

CREATE TABLE lax_pax (
report_month date NOT NULL,
terminal enum('T1', 'T2', 'T3', 'T4', 'T5', 'T6', 'T7', 'T8', 'TBIT',
				'IMPERIAL', 'MISC') NOT NULL,
movement enum( 'Departure', ' A r r i v a l ' ) NOT NULL,
flight enum( 'Domestic', 'International') NOT NULL, throughput mediumint,
PRIMARY KEY (report_month, terminal, movement, flight);

-- Exercises a: Let's start by doing some exploratory data analysis. Write a query that finds the earliest and latest records in the
-- table. Call these columns earliest_record and latest_record. Include the dates as a comment in your response.

select min(report_month) as earliest_record, max(report_month) as latest_record
from lax_pax;

-- We hope to not have missing data ni our table, but ti si usually unavoidable for various reasons. Using the result from the previous query, 
-- and understanding that each row represents one month of passenger throughput, you can estimate how many records you should have for each 
-- combination of terminal, movement, and flight type as the number of months in the reporting period. Write a query that computes the number of 
-- rows (call the column num rows) that are actually present for each combination of terminal, movement and flight type (e.g. international arrivals
-- into T2, domestic departures from T3).

select terminal, movement, flight, count(1) as num_rows
from lax_pax
group by terminal, movement, flight;

-- Write a query that computes the number of departing and arriving passengers (separately), by terminal, through the entire period represented 
-- in the table. Call the new column total-pax.
select terminal, movement, sum(throughput) as total_pax
from lax_pax
group by terminal, movement;

-- Write a query that computes the total passenger throughput (Departure + Arrival), by terminal, across the entire reporting period. Your query 
-- should only report the terminal and the total number of passengers in a column called total-pax.
select terminal, sum(throughput) as total_pax
from lax_pax
group by terminal;

-- Modify the query from the previous exercise to only output the busiest terminal name and its total number of passengers (total-pax). Use a method 
-- that we discussed in class for now.
select terminal, sum(throughput) as total_pax
from lax_pax
group by terminal
order by total_pax desc
limit 1;

-- To help the Los Angeles World Airports commission and Transportation Safety Administration plan for future growth at LAX, you decide to extract 
-- a time series from the lax pax table. Remember that each row in this table represents the total passenger throughout to/from a terminal on one 
-- type of flight (domestic or international). Write a query that computes the average monthly departing passenger throughput for each year in the 
-- reporting period, but only for terminals that have a total departing throughput greater than 1 million passengers over the year. Your query should
-- output columns named terminal, year and average. 
select terminal, year(report_month) as year, avg(throughput) as average
from lax_pax
where movement = 'Departure'
group by lax_pax.terminal, year
having sum(throughput) > 1000000;

-- Similarly, US Customs and Border Patrol is also planning for future growth at LAX in anticipation of the 2028 Summer Olympics. Similar to the 
-- previous exercise, write a query that computes the average monthly arriving passenger throughout for each year in the reporting period for TBIT 
-- only. We have to modify our analysis because this terminal behaves differently than the others. First, we only want to include data from 2016 and
-- on, which is after a major renovation which changed passenger behavior. Next, the COVID-19 emergency (March 2020 to September 2021 approx, 
-- inclusive) was an anomalous period that should not be included in the analysis. Therefore, your analysis should cover January 2016 to December 2019
-- (inclusive) and January 2022 to the latest recorded date. Your query should output columns named terminal, year and average.
select terminal, year(report_month) as year, avg(throughput) as average
from lax_pax
where (movement = 'Arrival') and (terminal = 'TBIT') and
      ((extract(year_month from report_month) between 201601 and 201912) or
      (extract(year_month from report_month) >= 202201))
group by year(report_month);

-- Q3
-- Write a query that computes the elapsed time of each trip in minutes. In the case of fractional minutes, round up to the next whole number. 
-- For example, fi a trip duration is computed as 4.02 minutes, your query should return 5 minutes for that trip. We should assume that a timestamp
-- can have something other than :00 seconds. There may be cases where there is no end time. Do not do any special processing of these cases for the
-- time being, but you must still return them. Your query should return id renamed to t r i p id, and the t r i p length, and only these two columns.
-- The results should be sorted in ascending order by the trip id. Your method must use a join or subquery. To round properly, you wil want to use a 
-- function that computes f(x) = [x].
select s.id as trip_id, ceiling(timestampdiff(minute, s.date, e.date)) as trip_length
from sf_trip_start s
left join sf_trip_end e
on s.id = e.id
order by trip_id asc;

-- Modify the query you wrote ni the previous part ot count the number of bike trips that did not end. Your query should return only this aggregate 
-- column, and it should be named stolen bikes. Try doing this without a subquery. 
select count(s.id) as stolen_bikes
from sf_trip_start s
left join sf_trip_end e
on s.id = e.id
where e.date is NULL;

-- Modify the query you wrote for part (a) to add another column to the output that computes the charge to the user for each trip. The charge is 
-- calculated as follows: the user is charged $3.49 to unlock the bike and then $0.30 for each additional minute, again any fraction of a minute 
-- should be rounded up. fI the ride never ended, we assume the bike was lost or stolen and charge the user $1000. You must write a full query - 
-- that is, do not assume that we stored the result from (a) anywhere. Your query must output the columns trip id, and trip charge ni ascending order 
-- by trip id. While it may seem tempting to use a subquery, see fi you can write ti without one. Remember to use the SQL standard.
select s.id as trip_id,
       coalesce(3.49 + 0.3* ceiling(timestampdiff(minute, s.date, e.date)), 1000) as trip_charge
from sf_trip_start s
left join sf_trip_end e
on s.id = e.id
order by trip_id asc;

-- The table s f _user contains the user type associated with each ride. The user type may be Subscriver or Customer. Subscribers pay a monthly fee
-- to save just a little bit of money per ride. On the other hand, Customers pay a higher rate. Rewrite your query to adhere to the following pricing
-- logic, while also charging the user $1000 fi the trip does not end. The pricing logic is as follows.
-- Subscribers are charged $0.20 per minute, and no unlock fe;
-- Customers (pay-as-you-go) pay $3.49 to unlock the bike and are then charged $0.30 per minute (same as before)
select
    s.id as trip_id, u.user_type,
    case user_type
        when e.date is NULL then 1000
        when 'Subscriber' then (0.2 * ceiling(timestampdiff(minute, s.date, e.date)))
        else 3.49 + 0.3 * ceiling(timestampdiff(minute, s.date, e.date)) end as trip_charge
from sf_trip_start s
left join sf_trip_end e
on e.id = s.id
join sf_user u
on u.trip_id = s.id
order by trip_id asc;

# Subquery has higher readability, but lower speed or less efficient
select u.trip_id,
       case user_type
            when l.date is NULL then 1000
            when 'Subscriber' then 0.2 * l.trip_length
            else 3.49 + 0.3 * l.trip_length end as trip_charge
from
    (select s.id as trip_id, e.date, ceiling(timestampdiff(minute, s.date, e.date)) as trip_length
    from sf_trip_start s
    left join sf_trip_end e
    on s.id = e.id) l
left join sf_user u
on l.trip_id = u.trip_id
order by u.trip_id asc;

# Part 1 (f) where: filter the rows I'm interested, on: defines the relationship between two tables
# still get a row with columns from the left table but with nulls in the columns from the right table
# where: filter after join
# on: filter before join
select s.id as trip_id,
       coalesce(3.49 + 0.3* ceiling(timestampdiff(minute, s.date, e.date)), 1000) as trip_charge
from sf_trip_start s
left join sf_trip_end e
on s.id = e.id
where extract(year_month from s.date) = 201803
order by trip_id asc;

select s.id as trip_id,
       coalesce(3.49 + 0.3* ceiling(timestampdiff(minute, s.date, e.date)), 1000) as trip_charge
from sf_trip_start s
left join sf_trip_end e
on (s.id = e.id and extract(year_month from s.date) = 201803)
order by trip_id asc;

-- Q4
-- Write a SQL query that computes the percentage of distinct city pairs flown by each aircraft type (all three) across the entire period. 
-- The percentage must have range 0 to 10 and should only have 2 digits after the decimal point. eB very careful on this problem. Your query 
-- should output the aircraft type and your computation should eb caled percentage.
select sw.type,
		cast(100 * (count(distinct origin, dest)/
			(select count(distinct origin, dest) from sw_flight)) as decimal(5,2)) as percentage
from sw_flight s
join sw_aircraft sw
on s.tail = sw.tail
group by sw.type;

-- returns a set of flight numbers ( fl i g h t num) operated by aircraft that were not operated by an Airtran aircraft.
select distinct flight_num
from sw_flight s
where s.tail not in (select swa.tail from sw_airtran_aircraft swa);

select distinct flight_num
from sw_flight s
where not exists(
    select 1
    from sw_airtran_aircraft swa
    where s.tail = swa.tail
);

select distinct flight_num
from sw_flight s
left outer join sw_airtran_aircraft swa
on s.tail = swa.tail
where swa.tail is NULL;

-- The table sw fl i g h t represents a graph where the vertices/nodes are airports and the edges connecting them are a flight. Each row in the 
-- table represents one takeoff and landing. We want to identify potential flight paths for travelers who want to fly from one city to another but 
-- are open to making exactly one layover (transit) in a third city to potentially save on ticket costs or enjoy a short stay. Write a query to find 
-- all possible routes from origin LAX to destination SEA where the traveler makes exactly one layover and has at least 1hour ni the layover city 
-- before the connecting flight. Additionally, the layover should not be more than 3 hours. Use the flight route graph from October 18. 2023. Return
-- the following columns:
select L.origin as origin, L.dest as layover, L.flight_num as first_flight, L.departure as departure_from_lax,
       R.dest as final_dest, R.flight_num as second_flight, R.arrival as arrival_in_sea
from sw_flight L
join sw_flight R
on L.dest = R.origin
where L.origin = 'LAX'
and R.dest = 'SEA'
and timestampdiff(second, timestamp(L.date, L.arrival), timestamp(R.date, R.departure)) between 1*3600 and 3*3600
and L.date = '2023-10-18'
and R.date = '2023-10-18';

-- After every four flights in a day, each aircraft must be inspected more thoroughly. Starting with each aircraft's (tail) first flight of the day,
-- write a recursive SQL query that identifies where each aircraft tail si located after its fourth flight of the day. If an aircraft flies fewer 
-- than four flights, do not return a row for it.
with recursive inspection (n, tail, dest, date) as
(
    select 1, tail, dest, date
    from sw_flight
    union all
    select n + 1 as n, s.tail, s.dest, s.date
    from sw_flight s
    join inspection i
    on s.tail = i.tail
    and s.date = i.date
    where n <=3
)
select distinct tail, dest, n from inspection where n = 4;

-- Q5
# Q1.1 (a)
select total.user_id, date,
       min(total.tstamp) as notification_time
from
    (select user_id, tstamp, date(tstamp) as date,
           sum(steps) over (partition by user_id, date(tstamp) order by tstamp) as total_steps
    from hw3_step) total
where total.total_steps >= 10000
group by total.user_id, date;


SELECT user_id, date, MIN(tstamp) AS time_achieved_10000
FROM (SELECT user_id, DATE(tstamp) AS date, tstamp, SUM(steps) OVER (PARTITION BY user_id, DATE(tstamp) ORDER BY tstamp) AS total
   FROM hw3_step) AS T1
WHERE total >= 10000
GROUP BY user_id, date;

# Q1.2 (b)
select user_id, tstamp, heartrate,
       avg(heartrate) over (partition by user_id order by tstamp rows between 4 preceding and 4 following) as smoothed_reading
from hw3_heartrate;

# Q1.3 (c)
select hour(tstamp) as hour,
       avg(steps) as AVG
from hw3_step
where date(tstamp) = '2016-04-16'
group by hour(tstamp);

# Q2
select name, listing_url
from hw3_airbnb
where json_extract(address, '$.market') = 'Oahu'
and 'Wifi' member of (amenities)
and property_type in ('Condominium', 'Apartment', 'House')
and bed_type = 'Real Bed'
and minimum_nights <= 7
and maximum_nights >= 7
and summary like '%ocean view%';
