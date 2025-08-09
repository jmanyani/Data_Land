# This Spark program calculates the highest recorded temperature
# for each weather station in a historical weather dataset.

from pyspark import SparkConf, SparkContext

# Configure Spark for local execution
spark_config = SparkConf().setMaster("local").setAppName("WeatherStationMaxTemps")

# Initialize SparkContext
spark_context = SparkContext(conf=spark_config)

# Function to parse each line into (stationID, entryType, temperatureF)
def parse_weather_line(record):
    columns = record.split(',')
    station_id = columns[0]
    record_type = columns[2]
    # Convert tenths of Celsius to Fahrenheit
    temperature_f = float(columns[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (station_id, record_type, temperature_f)

# Load dataset (placeholder path used)
weather_lines = spark_context.textFile("XXXXX")

# Parse the raw CSV data
parsed_weather_data = weather_lines.map(parse_weather_line)

# Keep only TMAX (maximum temperature) records
max_temp_records = parsed_weather_data.filter(lambda entry: "TMAX" in entry[1])

# Map to (stationID, temperatureF)
station_max_pairs = max_temp_records.map(lambda entry: (entry[0], entry[2]))

# Find the highest temperature per station
max_temps_per_station = station_max_pairs.reduceByKey(lambda t1, t2: max(t1, t2))

# Collect results
final_max_temps = max_temps_per_station.collect()

# Print station ID and maximum temperature
for station, temp in final_max_temps:
    print(f"{station}\t{temp:.2f}F")
