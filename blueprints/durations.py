# blueprints/durations.py
from flask import Blueprint, jsonify
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
import csv, io
from flask import Response
import config
from config import jsonpath
import json

# Create a blueprint for durations
durations_bp = Blueprint('durations', __name__)

# Load the JSON schema
with open(jsonpath / 'durations.json') as f:
    durations_schema = json.load(f)

def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix."""
    return f"{base_name}{config.SUFFIX}"

@durations_bp.route('/v1/durations', methods=['GET'])
def get_all_durations():
    """Return a list of all durations."""
    print("get_all_durations called")
    
    columns = durations_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(durations_schema['table_info']['name'])}"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        duration_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    writer.writerow(column_names)  # Write header

    # Write rows to CSV
    for row in duration_rows:
        writer.writerow([row[col] for col in column_names])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_durations.csv"})

@durations_bp.route('/v1/durations/<string:duration_name>', methods=['GET'])
def get_duration_data(duration_name):
    """Return details for a specific duration."""
    print(f"get_duration_data called for duration_name: {duration_name}")
    
    columns = durations_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(durations_schema['table_info']['name'])} WHERE duration_name = :duration_name"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"duration_name": duration_name})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Duration not found"}, 404
        return jsonify(rows[0])