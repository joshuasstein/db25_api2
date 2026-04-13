# blueprints/loads.py
from flask import Blueprint, jsonify
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
import csv, io
from flask import Response
import config
from config import jsonpath
import json

# Create a blueprint for loads
loads_bp = Blueprint('loads', __name__)

# Load the JSON schema
with open(jsonpath / 'loads.json') as f:
    loads_schema = json.load(f)

def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix."""
    return f"{base_name}{config.SUFFIX}"

@loads_bp.route('/v1/loads', methods=['GET'])
def get_all_loads():
    """Return a list of all loads."""
    print("get_all_loads called")
    
    columns = loads_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(loads_schema['table_info']['name'])}"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        load_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    writer.writerow(column_names)  # Write header

    # Write rows to CSV
    for row in load_rows:
        writer.writerow([row[col] for col in column_names])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_loads.csv"})

@loads_bp.route('/v1/loads/<int:load_id>', methods=['GET'])
def get_load_data(load_id: int):
    """Return details for a specific load."""
    print(f"get_load_data called for load_id: {load_id}")
    
    columns = loads_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(loads_schema['table_info']['name'])} WHERE load_id = :load_id"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"load_id": load_id})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Load not found"}, 404
        return jsonify(rows[0])