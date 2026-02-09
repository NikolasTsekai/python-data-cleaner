import csv
import os

def process_production_data(input_file, output_file):
    print(f"--- Starting ETL Process for {input_file} ---")
    cleaned_data = []
    total_temperature = 0
    valid_records = 0

    if not os.path.exists(input_file):
        with open(input_file, 'w') as f:
            f.write("BatchID,Temperature,Status\n101,45.5,OK\n102,120.0,ERROR\n103,50.2,OK")

    with open(input_file, 'r') as f_in:
        reader = csv.DictReader(f_in)
        for row in reader:
            temp = float(row['Temperature'])
            if 20 <= temp <= 80:
                cleaned_data.append(row)
                total_temperature += temp
                valid_records += 1

    avg_temp = total_temperature / valid_records if valid_records > 0 else 0
    print(f"Average Temp: {avg_temp:.2f} C")

    with open(output_file, 'w', newline='') as f_out:
        if cleaned_data:
            writer = csv.DictWriter(f_out, fieldnames=cleaned_data[0].keys())
            writer.writeheader()
            writer.writerows(cleaned_data)

if __name__ == "__main__":
    process_production_data('raw_data.csv', 'clean_data.csv')
