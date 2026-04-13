# blueprints/units.py
from flask import Blueprint, jsonify
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
import csv, io
from flask import Response
import config
from config import jsonpath
import json


# Create a blueprint for units
units_bp = Blueprint('units', __name__)

# Load the JSON schema
with open(jsonpath / 'units.json') as f:
    units_schema = json.load(f)

def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix, if applicable."""
    return f"{base_name}{config.SUFFIX}"

@units_bp.route('/v1/units', methods=['GET'])
def get_all_units():
    """Return a list of all units."""
    print("get_all_units called")
    
    columns = units_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(units_schema['table_info']['name'])}"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        unit_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    writer.writerow(column_names)  # Write header

    # Write rows to CSV
    for row in unit_rows:
        writer.writerow([row[col] for col in column_names])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_units.csv"})

@units_bp.route('/v1/units/<string:unit_name>', methods=['GET'])
def get_unit_data(unit_name):
    """Return details for a specific unit."""
    print(f"get_unit_data called for unit_name: {unit_name}")
    
    columns = units_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(units_schema['table_info']['name'])} WHERE unit_name = :unit_name"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"unit_name": unit_name})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Unit not found"}, 404
        return jsonify(rows[0])