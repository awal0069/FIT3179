import csv

input_file = "FIT3179/data/forest-area-as-share-of-land-area.csv"
output_file = "FIT3179/data/forest_area_from_1990.csv"

with open(input_file, "r", newline="", encoding="utf-8") as infile, \
     open(output_file, "w", newline="", encoding="utf-8") as outfile:

    reader = csv.reader(infile)
    writer = csv.writer(outfile)

    header = next(reader)
    writer.writerow(header)

    # Find the index of the "Year" column
    try:
        year_index = header.index("Year")
    except ValueError:
        raise Exception("No 'Year' column found in CSV header.")

    # Write only rows where Year >= 1990
    for row in reader:
        try:
            if int(row[year_index]) >= 1990:
                writer.writerow(row)
        except ValueError:
            # Skip rows where the year value isn't numeric
            continue

print(f"✅ Filtered data written to '{output_file}'")
