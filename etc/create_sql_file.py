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
