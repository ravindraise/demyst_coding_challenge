import json
import random
import string
import csv
import os


# Load the specification from the JSON file
def load_spec():
    with open('spec.json', 'r') as f:
        spec = json.load(f)
    return spec


# Function to generate random data for each column based on the given width
def gen_fixed_width_row(offsets):
    row = []
    for width in offsets:
        row.append(''.join(random.choices(string.ascii_letters + string.digits, k=int(width))))
    return ''.join(row)


# Function to generate a fixed-width file
def gen_fixed_width_file(spec):
    fixed_width_file = 'fixed_width/fixed_width_data.txt'
    with open(fixed_width_file, 'w', encoding=spec["FixedWidthEncoding"]) as f:
        if spec["IncludeHeader"].lower() == "true":
            header = ''.join([col.ljust(int(width)) for col, width in zip(spec["ColumnNames"], spec["Offsets"])])
            f.write(header + '\n')

        # Generate and write rows
        num_rows = 1000
        # You can specify the number of rows you want to generate
        if "NoOfRows" in spec:
            num_rows = spec["NoOfRows"]
        for _ in range(num_rows):
            row = gen_fixed_width_row(spec["Offsets"])
            f.write(row + '\n')

    print(f"Fixed-width file '{fixed_width_file}' generated successfully.")


# Function to parse a fixed-width row based on the offsets
def parse_fixed_width_row(row, offsets):
    parsed_row = []
    start = 0
    for width in offsets:
        parsed_row.append(row[start:start + int(width)].strip())
        start += int(width)
    return parsed_row


# Function to parse the fixed-width file and convert to CSV
def parse_fixed_width_to_csv(spec):
    fixed_width_file = 'fixed_width/fixed_width_data.txt'
    parsed_csv_file = 'fixed_width/parsed_data.csv'
    with open(fixed_width_file, 'r', encoding=spec["FixedWidthEncoding"]) as infile, \
            open(parsed_csv_file, 'w', newline='', encoding=spec["DelimitedEncoding"]) as outfile:

        reader = infile.readlines()
        writer = csv.writer(outfile, delimiter=',')

        # Write the header if it exists
        if spec["IncludeHeader"].lower() == "true":
            header = parse_fixed_width_row(reader[0], spec["Offsets"])
            writer.writerow(header)
            reader = reader[1:]  # Skip the header row

        # Parse and write each row
        for line in reader:
            parsed_row = parse_fixed_width_row(line, spec["Offsets"])
            writer.writerow(parsed_row)

    print(f"CSV file '{parsed_csv_file}' generated successfully.")


if __name__ == "__main__":
    os.makedirs("fixed_width", exist_ok=True)
    file_spec = load_spec()
    gen_fixed_width_file(file_spec)
    parse_fixed_width_to_csv(file_spec)
