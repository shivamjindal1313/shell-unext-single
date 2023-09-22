#!/bin/bash
 
# Define the paths and filenames
existing_zip="SparkApp.zip"
new_zip="SparkApp.zip"
files_to_zip="SparkApp.py input_data.csv"  # Add the files you want to include in the new zip

 

# Check if the existing zip file exists
if [ -e "$existing_zip" ]; then
    # If it exists, delete the existing zip file
    rm "$existing_zip"
fi

 

# Create a new zip file with the specified files
zip -r "$new_zip" $files_to_zip

 

echo "New zip file '$new_zip' created."
