#Daily Min Temp (by city)
SELECT city, date, mintemp 
FROM weather_data 
WHERE date = CURRENT_DATE - INTERVAL '1' DAY;

#Daily Max Temp (by city)
SELECT city, date, maxtemp 
FROM weather_data 
WHERE date = CURRENT_DATE - INTERVAL '1' DAY;

#Daily Sunset (by city)
SELECT city, date, sunset 
FROM weather_data 
WHERE date = CURRENT_DATE - INTERVAL '1' DAY;

#Daily Sunrise (by city)
SELECT city, date, sunrise 
FROM weather_data 
WHERE date = CURRENT_DATE - INTERVAL '1' DAY;

#Hourly Temp (by city)
SELECT city, date, hourly 
FROM weather_data 
WHERE date = CURRENT_DATE - INTERVAL '1' DAY;
