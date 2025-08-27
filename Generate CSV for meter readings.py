
import csv
import random

# set number of houses and meter readings

max_houses = 1000
readings_perhouse = 2880 # 24 (24hrs/day) x 2 (half hourly) x 60days(2mths) = 2880

# Generate data
data = [] # use list to save all the data
for i in range (1, max_houses+1):
    house_id = f"H{i:03d}" 
    meter_id = f"12323{i:04d}"
    
    initial_reading = 5 + random.uniform(0, 10) # first meter reading
    next_reading = initial_reading    
    
    for j in range (readings_perhouse - 1): # initial reading was determined, so remove 1
        next_reading += (5 + random.uniform(0, 10)) # add randomness to next reading
        data.append([house_id, meter_id, round(next_reading,2)]) # create csv file containing house_id, meter_id and reading 

# generate the file

csv_filename = "project_meter_readings.csv"

with open(csv_filename, mode = 'w', newline = "") as file:
    writer = csv.writer(file)
    writer.writerow(["HouseID", "MeterID", "Meter Readings"])
    writer.writerows(data)
    
