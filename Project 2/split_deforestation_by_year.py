import csv

# Input and output file paths
years = [1990, 2000, 2010, 2015]
input_file = "Project 2/data/annual-deforestation.csv"
output_files = {year: f"Project 2/data/deforestation{year}.csv" for year in years}

# Read the input CSV and split rows by year
with open(input_file, newline='', encoding='utf-8') as infile:
    reader = csv.DictReader(infile)
    # Prepare writers for each year
    writers = {}
    outfiles = {}
    for year in years:
        outfiles[year] = open(output_files[year], 'w', newline='', encoding='utf-8')
        writers[year] = csv.DictWriter(outfiles[year], fieldnames=reader.fieldnames)
        writers[year].writeheader()
    # Write rows to the appropriate file
    for row in reader:
        year_val = int(row['Year'])
        if year_val in years:
            writers[year_val].writerow(row)
    # Close all output files
    for f in outfiles.values():
        f.close()

print("Files created:", list(output_files.values()))
