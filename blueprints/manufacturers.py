# blueprints/manufacturers.py
from flask import Blueprint, jsonify
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
import csv, io
from flask import Response
import config
from config import jsonpath
import json

# Create a blueprint for manufacturers
manufacturers_bp = Blueprint('manufacturers', __name__)

# Load the JSON schema
with open(jsonpath / 'manufacturers.json') as f:
    manufacturers_schema = json.load(f)

def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix."""
    return f"{base_name}{config.SUFFIX}"

@manufacturers_bp.route('/v1/manufacturers', methods=['GET'])
def get_all_manufacturers():
    """Return a list of all manufacturers."""
    print("get_all_manufacturers called")
    
    columns = manufacturers_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(manufacturers_schema['table_info']['name'])}"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        manufacturer_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    writer.writerow(column_names)  # Write header

    # Write rows to CSV
    for row in manufacturer_rows:
        writer.writerow([row[col] for col in column_names])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_manufacturers.csv"})

@manufacturers_bp.route('/v1/manufacturers/<string:manufacturer_name>', methods=['GET'])
def get_manufacturer_data(manufacturer_name):
    """Return details for a specific manufacturer."""
    print(f"get_manufacturer_data called for manufacturer_name: {manufacturer_name}")
    
    columns = manufacturers_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(manufacturers_schema['table_info']['name'])} WHERE manufacturer_name = :manufacturer_name"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"manufacturer_name": manufacturer_name})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Manufacturer not found"}, 404
        return jsonify(rows[0])