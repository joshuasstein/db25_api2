# blueprints/sites.py
from flask import Blueprint, jsonify
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
import csv, io
from flask import Response
import config
from config import jsonpath
import json

# Create a blueprint for sites
sites_bp = Blueprint('sites', __name__)

# Load the JSON schema
with open(jsonpath / 'sites.json') as f:
    sites_schema = json.load(f)

def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix."""
    return f"{base_name}{config.SUFFIX}"

@sites_bp.route('/v1/sites', methods=['GET'])
def get_all_sites():
    """Return a list of all sites."""
    print("get_all_sites called")
    
    columns = sites_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(sites_schema['table_info']['name'])}"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        site_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    writer.writerow(column_names)  # Write header

    # Write rows to CSV
    for row in site_rows:
        writer.writerow([row[col] for col in column_names])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_sites.csv"})

@sites_bp.route('/v1/sites/<string:site_name>', methods=['GET'])
def get_site_data(site_name):
    """Return details for a specific site."""
    print(f"get_site_data called for site_name: {site_name}")
    
    columns = sites_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(sites_schema['table_info']['name'])} WHERE site_name = :site_name"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"site_name": site_name})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Site not found"}, 404
        return jsonify(rows[0])