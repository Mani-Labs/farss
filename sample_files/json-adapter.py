#!/usr/bin/env python3
# File: json_adapter.py
# Version: 1.0.0
# Last Updated: 2025-05-01 10:00
# Changelog: Initial version that converts new JSON format to be compatible with frontend

import json
import os
import sys
from datetime import datetime

def convert_json_format(input_file, output_file=None):
    """
    Converts new JSON format to match the structure expected by the Farsiland frontend.
    
    Args:
        input_file (str): Path to the input JSON file
        output_file (str): Path to the output JSON file. If None, will use input filename + "_frontend.json"
    
    Returns:
        str: Path to the created output file
    """
    # Set default output filename if not provided
    if output_file is None:
        file_name, file_ext = os.path.splitext(input_file)
        output_file = f"{file_name}_frontend{file_ext}"
    
    # Read the input JSON file
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: Input file '{input_file}' not found")
        return None
    except json.JSONDecodeError:
        print(f"Error: Input file '{input_file}' is not valid JSON")
        return None
    
    # Create the new structure
    frontend_data = {
        "metadata": data.get("metadata", {}),
        "Movies": {},
        "TV Shows": {},
        "episodes": []
    }
    
    # Convert movies array to object with title keys
    for movie in data.get("movies", []):
        title = movie.get("title_en", "")
        if title:
            frontend_data["Movies"][title] = movie
    
    # Convert shows array to object with title keys and adjust episodes structure
    for show in data.get("shows", []):
        title = show.get("title_en", "")
        if title:
            # Copy the show but remove seasons to re-add them with proper structure
            show_copy = {k: v for k, v in show.items() if k != "seasons"}
            
            # Create empty episodes array if not already present
            if "episodes" not in show_copy:
                show_copy["episodes"] = []
            
            # Process seasons and flatten episodes
            for season in show.get("seasons", []):
                season_num = season.get("season_number")
                
                # Add all episodes from this season to the show's episodes array
                for episode in season.get("episodes", []):
                    # Add season number to episode if not already present
                    if "season" not in episode:
                        episode["season"] = season_num
                    
                    # Add to the show's episodes array
                    show_copy["episodes"].append(episode)
            
            # Add to TV Shows object
            frontend_data["TV Shows"][title] = show_copy
    
    # Copy episodes array (if present)
    frontend_data["episodes"] = data.get("episodes", [])
    
    # Write to output file
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(frontend_data, f, ensure_ascii=False, indent=2)
        print(f"Successfully converted JSON to frontend format: {output_file}")
        return output_file
    except Exception as e:
        print(f"Error writing output file: {e}")
        return None

def main():
    """Main function to parse command line args and run the converter"""
    if len(sys.argv) < 2:
        print("Usage: python json_adapter.py input_file.json [output_file.json]")
        return
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    convert_json_format(input_file, output_file)

if __name__ == "__main__":
    main()
