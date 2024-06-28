CREATE TABLE weather_data (
    date DATE, 
    city TEXT,
    sunrise TIME, 
    sunset TIME, 
    moonrise TIME, 
    moonset TIME, 
    moon_phase TEXT, 
    moon_illumination INTEGER, 
    mintemp REAL, 
    maxtemp REAL, 
    avgtemp REAL, 
    totalsnow REAL, 
    sunhour REAL, 
    uv_index REAL,
    hourly TEXT,
    PRIMARY KEY (date, city)
);

CREATE INDEX idx_city_date ON weather_data(city, date);
CREATE INDEX idx_date_city ON weather_data(date, city);
CREATE INDEX idx_city ON weather_data(city);
CREATE INDEX idx_date ON weather_data(date);
