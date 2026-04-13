# blueprints/modules.py
from flask import Blueprint, jsonify
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
import csv, io
from flask import Response
import config
from config import jsonpath
import json

# Create a blueprint for modules
modules_bp = Blueprint('modules', __name__)

# Load the JSON schema
with open(jsonpath / 'modules.json') as f:
    modules_schema = json.load(f)

def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix."""
    return f"{base_name}{config.SUFFIX}"

@modules_bp.route('/v1/modules', methods=['GET'])
def get_all_modules():
    """Return a list of all modules."""
    print("get_all_modules called")
    
    columns = modules_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(modules_schema['table_info']['name'])}"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        module_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    writer.writerow(column_names)  # Write header

    # Write rows to CSV
    for row in module_rows:
        writer.writerow([row[col] for col in column_names])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_modules.csv"})

@modules_bp.route('/v1/modules/<int:psel_id>', methods=['GET'])
def get_module_data(psel_id):
    """Return details for a specific module."""
    print(f"get_module_data called for psel_id: {psel_id}")
    
    columns = modules_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(modules_schema['table_info']['name'])} WHERE psel_id = :psel_id"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"psel_id": psel_id})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Module not found"}, 404
        return jsonify(rows[0])