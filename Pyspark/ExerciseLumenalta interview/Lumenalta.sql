Problem Statement

Your task is to prepare a list of cities with the date of last reservation made in the city and a main photo (photos[0]) of the most popular (by number of bookings) hotel in this city.

Sort results in ascending order by city. If many hotels have the same amount of bookings sort them by ID (ascending order). Remember that the query will also be run of different datasets.
Task description

create table city

id int primary key, 
name varchar(50) not null

create table hotel

id int primary key, 
city_id int not null references city, 
name varchar(50) not null, 
day_price numeric(8, 2) not null, 
photos jsonb default '[]'

create table booking

id int primary key,
hotel id int not null references hotel, 
booking date date not null, 
start_date  date not null, 
end_date date not null

-- Step 1: Identify the last reservation date for each city
WITH LastReservation AS (
    SELECT 
        c.id AS city_id, 
        c.name AS city_name,
        MAX(b.booking_date) AS last_reservation_date
    FROM 
        city c
        JOIN hotel h ON c.id = h.city_id
        JOIN booking b ON h.id = b.hotel_id
    GROUP BY 
        c.id, c.name
),

-- Step 2: Determine the most popular hotel in each city
HotelPopularity AS (
    SELECT 
        h.city_id, 
        h.id AS hotel_id, 
        h.photos,
        COUNT(b.id) AS booking_count
    FROM 
        hotel h
        JOIN booking b ON h.id = b.hotel_id
    GROUP BY 
        h.city_id, h.id
),

-- Step 3: Rank hotels by popularity and select the most popular one
RankedHotels AS (
    SELECT 
        hp.city_id,
        hp.hotel_id,
        hp.photos,
        ROW_NUMBER() OVER (PARTITION BY hp.city_id ORDER BY hp.booking_count DESC, hp.hotel_id ASC) AS rank
    FROM 
        HotelPopularity hp
),

-- Step 4: Extract the main photo from the most popular hotel's photos array
MainPhoto AS (
    SELECT 
        rh.city_id,
        rh.hotel_id,
        (SELECT jsonb_array_elements_text(rh.photos) LIMIT 1)::TEXT AS main_photo -- Extract the first element from the JSON array and cast to TEXT
    FROM 
        RankedHotels rh
    WHERE 
        rh.rank = 1
)

-- Final Step: Combine results and sort by city name
SELECT 
    lr.city_name,
    lr.last_reservation_date,
    mp.main_photo,
    mp.hotel_id
FROM 
    LastReservation lr
    JOIN MainPhoto mp ON lr.city_id = mp.city_id
ORDER BY 
    lr.city_name ASC;
