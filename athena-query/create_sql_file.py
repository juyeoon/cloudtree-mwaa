import os


def create_sql_files(prefix, filenames, directory="athena-query"):
    """
    Creates .sql files with the given prefix and filenames.

    Args:
        prefix (str): Prefix for the filenames.
        filenames (list of str): List of filenames (without extensions).
        directory (str): Directory where files will be created (default: 'sql_files').

    Returns:
        None
    """
    # Ensure the directory exists
    if not os.path.exists(directory):
        os.makedirs(directory)

    for filename in filenames:
        # Combine prefix and filename to create the full file name
        full_filename = f"{prefix}{filename}.sql"
        file_path = os.path.join(directory, full_filename)

        # Check if the file already exists
        if os.path.exists(file_path):
            print(f"Skipped: {file_path} (already exists)")
            continue

        # Create the SQL file as an empty file
        with open(file_path, "w") as file:
            pass

        print(f"Created: {file_path}")


# Example usage
file_list1 = [
    "best_loan_list_raw",
    "bus_stop_loc_raw",
    "cultural_event_info_raw",
    "cultural_event_parse_raw",
    "library_data_raw",
    "subway_station_loc_raw",
    "city_park_info_raw",
]
create_sql_files("create_table_", file_list1)

file_list2 = [
    "library_data",
    "city_park_info",
    "bus_stop_loc",
    "cultural_event_info",
    "subway_station_loc",
    "best_loan_list",
]
create_sql_files("create_table_", file_list2)
