#!/usr/local/bin/python3.11
import csv
import os
import humanize as hm
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
# MARKER_40C = '#######################################'
MARKER_CHAR = '#'
BASE_PATH = os.getenv('TORBASE')
OUTPUT_FILE = os.path.join(BASE_PATH, 'Media_Index_v1.0.csv')
############################################################################


############################################################################
def load_shell_environment(profile_path="/Users/scott/.bash_profile"):
    # Use subprocess to source the shell profile and print the environment variables
    command = f"source {profile_path} && env"
    proc = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True, executable='/bin/bash')
    for line in proc.stdout:
        (key, _, value) = line.decode("utf-8").partition("=")
        os.environ[key] = value.strip()

############################################################################


############################################################################
def get_media_archive_paths():
    result = subprocess.run(
        ['find', '/Volumes', '-type', 'd', '-maxdepth', '2', '-name', 'Media_Archive'],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )

    # Split the output into lines
    paths = result.stdout.strip().split('\n')
    print('{:>30}:\t {:<14}'.format('Total number of paths found', len(paths)))

    # Create the dictionary
    media_archive_dict = {}
    for path in paths:
        if path:  # Ensure the path is not empty
            parts = [x for x in path.strip().split('/') if len(x) > 0]
            if len(parts) >= 2:  # Ensure there are enough parts to extract the disk label
                media_archive_dict[parts[1]] = path
    print('{:>30}:\t {:<14}'.format('Disk labels', str(sorted(list(media_archive_dict.keys())))))
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
                print('\n{:>35} {:20} {:<30}'.format(f'{MARKER_CHAR * 28}', ' DUPLICATION ALERT!! ', f'{MARKER_CHAR * 29}'))
                print('{:>30}:\t {:<14}'.format('Object', entry_name))
                print('{:>30}:\t {:<14}'.format('Existing item', dup_label[1]))
                print('{:>30}:\t {:<14}'.format('Current path', label))
                print('{:>87}\n'.format(f'{MARKER_CHAR * 80}'))
                entry_name += "_DUPLICATE"
            entries[entry_name] = i.resolve()
    return entries
############################################################################


############################################################################
def write_to_postgres(data):
    try:
        # Convert any PosixPath objects to strings in the data
        data = [(str(directory), str(filesystem_path)) for directory, filesystem_path in data]

        # Connect to database
        conn = psycopg2.connect(
#             host='localhost',    # Change as needed
            host='127.0.0.1',    # Change as needed
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

    except psycopg2.Error as e:
        print('{:>87}\n'.format(f'{MARKER_CHAR * 80}'))
        print('{:>30}:\t {:<14}'.format('PostgreSQL Error', str(e)))
        print('{:>87}\n'.format(f'{MARKER_CHAR * 80}'))
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
        print('{:>87}\n'.format(f'{MARKER_CHAR * 80}'))
        print('{:>30}:\t {:<14}'.format('IO Error', str(e)))
        print('{:>87}\n'.format(f'{MARKER_CHAR * 80}'))
############################################################################


############################################################################
def main():
    load_shell_environment()

    print(f'\n{MARKER_CHAR * 120}')
    print('{:>30}:\t {:<14}'.format('Starting execution at', str(dt.datetime.now())[:19]))

    # Dynamic query to collect all potential target storage media
    dirs_dict = get_media_archive_paths()

    # Collect item data across all Media_Archive locations
    sorted_dict = sorted(get_entries(dirs_dict).items())
    print('{:>30}:\t {:<14}'.format('Total items found this scan', len(sorted_dict)))

    # Insert index data to database
    print('{:>30}:\t {:<14}'.format('Inserting data into database', f'{os.getenv("PG_database")}/media_archive_index'))
    write_to_postgres(sorted_dict)

    # Write index data to CSV file
    print('{:>30}:\t {:<14}'.format('Writing data to', OUTPUT_FILE))
    write_to_csv(sorted_dict)

    print('\n{:>30}:\t {:<14}'.format('Total execution time', hm.precisedelta(dt.datetime.now() - START_TIME)))
    print(f'{MARKER_CHAR * 120}\n')
############################################################################

if __name__ == "__main__":
    main()
