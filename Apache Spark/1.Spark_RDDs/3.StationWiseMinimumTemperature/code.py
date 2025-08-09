# This Spark program calculates the minimum recorded temperature
# for each weather station from a historical weather dataset.

from pyspark import SparkConf, SparkContext

# Configure Spark to run locally with an application name
spark_config = SparkConf().setMaster("local").setAppName("WeatherStationMinTemps")

# Initialize SparkContext
spark_context = SparkContext(conf=spark_config)

# Function to parse each CSV line into (stationID, entryType, temperatureF)
def parse_weather_record(record):
    fields = record.split(",")
    station_id = fields[0]
    record_type = fields[2]
    # Convert temperature from tenths of Celsius to Fahrenheit
    temperature_f = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (station_id, record_type, temperature_f)

# Load dataset (placeholder path used here)
weather_data = spark_context.textFile("XXXXX")

# Map raw data to parsed tuples
parsed_weather = weather_data.map(parse_weather_record)

# Filter only TMIN (minimum temperature) records
min_temp_records = parsed_weather.filter(lambda record: "TMIN" in record[1])

# Keep only (stationID, temperature) pairs
station_temperature_pairs = min_temp_records.map(lambda record: (record[0], record[2]))

# Find the minimum temperature for each station
min_temps_by_station = station_temperature_pairs.reduceByKey(lambda t1, t2: min(t1, t2))

# Collect results into a list
final_min_temps = min_temps_by_station.collect()

# Print each station and its minimum recorded temperature
for station, temp in final_min_temps:
    print(f"{station}: {temp}")
