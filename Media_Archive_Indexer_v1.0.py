#!/usr/local/bin/python3.11
import csv
import os
import humanize
import datetime as dt
import subprocess
from pathlib import Path
import psycopg2

'''
Get item list for all Media_Archive locations. Insert into db & write to CSV file
'''
############################################################################
# Global variables
START_TIME = dt.datetime.now()
MARKER_40C = '#######################################'
MARKER_CHAR = '#'
BASE_PATH = os.getenv('TORBASE')
OUTPUT_FILE = os.path.join(BASE_PATH, 'Media_Index_v1.0.csv')
############################################################################


############################################################################
def get_media_archive_paths():
    result = subprocess.run(
        ['find', '/Volumes', '-type', 'd', '-maxdepth', '2', '-name', 'Media_Archive'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    # Split the output into lines
    paths = result.stdout.strip().split('\n')
    print(f'  Total number of paths found:\t {len(paths)}')

    # Create the dictionary
    media_archive_dict = {}
    for path in paths:
        if path:  # Ensure the path is not empty
            parts = [x for x in path.strip().split('/') if len(x) > 0]
            if len(parts) >= 2:  # Ensure there are enough parts to extract the disk label
                media_archive_dict[parts[1]] = path
    print(f'\t\t  Disk labels:\t {sorted(list(media_archive_dict.keys()))}')
    return media_archive_dict
############################################################################


############################################################################
def get_entries(paths):
    entries = {}
    for label, path in paths.items():
        for i in Path(path).iterdir():
            entry_name = i.name
            if entry_name == '.DS_Store':
                continue
            if entry_name in entries:
                dup_label = [x for x in str(entries[entry_name]).strip().split('/') if len(x) > 0]
                print(f'\n\t\t\t {MARKER_CHAR * 20} DUPLICATION ALERT!! {MARKER_CHAR * 20}')
                print(f'\t\t\t\t       Object:\t {entry_name}')
                print(f'\t\t\t\tExisting item:\t {dup_label[1]}')
                print(f'\t\t\t\t Current path:\t {label}')
                entry_name += "_DUPLICATE"
                print(f'\t\t\t {MARKER_CHAR * 19} END DUPLICATION ALERT {MARKER_CHAR * 19}')
            entries[entry_name] = i.resolve()
    return entries
############################################################################


############################################################################
def write_to_postgres(data):
    try:
        # Connect to database
        conn = psycopg2.connect(
            host='localhost',    # Change as needed
            user=os.getenv("PG_username"),
            password=os.getenv("PG_password"),
            dbname=os.getenv("PG_database")
        )
        cursor = conn.cursor()

        # Clear the existing table
        cursor.execute("TRUNCATE TABLE media_archive_index")

        # Insert data into the table
        insert_query = "INSERT INTO media_archive_index (item, filesystem_path) VALUES (%s, %s)"
        cursor.executemany(insert_query, data)

        # Commit the transaction
        conn.commit()

        # Close the cursor and connection
        if cursor:
            cursor.close()
        if conn:
            conn.close()

    except psycopg2.Error as err:
        print('***********************************************')
        print(f'\t PostgreSQL error: {err}')
        print('***********************************************')
        exit(0)
############################################################################


############################################################################
def write_to_csv(data):
    global OUTPUT_FILE
    try:
        with open(OUTPUT_FILE, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Directory', 'Filesystem_Path'])
            writer.writerows(data)
    except IOError as e:
        print('***********************************************')
        print(f'\tError opening "{OUTPUT_FILE}" for writing.')
        print(f'\tResponse: "{str(e)}"')
        print('***********************************************')
        print(f'Total Execution time:\t {humanize.precisedelta(dt.datetime.now() - START_TIME)}')
        print(f'{MARKER_CHAR * 120}\n')
        exit(0)
############################################################################


############################################################################
def main():
    print(f'\n{MARKER_CHAR * 120}')
    print(f'        Starting execution at:\t {str(dt.datetime.now())[:19]}')

    # Dynamic query to collect all potential target storage media
    dirs_dict = get_media_archive_paths()
    entries_dict = get_entries(dirs_dict)
    sorted_dict = sorted(entries_dict.items())
    print(f'\n  Total items found this scan:\t {len(sorted_dict)}')
    print(f'              Writing data to:\t {OUTPUT_FILE}')
    write_to_csv(sorted_dict)
    write_to_postgres(sorted_dict)
    print(f'         Total execution time:\t {humanize.precisedelta(dt.datetime.now() - START_TIME)}')
    print(f'{MARKER_CHAR * 120}\n')
############################################################################

if __name__ == "__main__":
    main()
