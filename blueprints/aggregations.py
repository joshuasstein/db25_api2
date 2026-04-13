# blueprints/aggregations.py
import json
import os
from flask import Blueprint, jsonify, Response
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
import csv, io
import config
from config import jsonpath

# Create a blueprint for aggregations
aggregations_bp = Blueprint('aggregations', __name__)

# Load the JSON schema
with open(jsonpath / 'aggregations.json') as f:
    aggregations_schema = json.load(f)

def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix."""
    return f"{base_name}{config.SUFFIX}"

@aggregations_bp.route('/v1/aggregations', methods=['GET'])
def get_all_aggregations():
    """Return a list of all aggregations."""
    print("get_all_aggregations called")
    
    columns = aggregations_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(aggregations_schema['table_info']['name'])}"

    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        aggregation_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    writer.writerow(column_names)  # Write header

    # Write rows to CSV
    for row in aggregation_rows:
        writer.writerow([row[col] for col in column_names])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_aggregations.csv"})

@aggregations_bp.route('/v1/aggregations/<string:aggregation_name>', methods=['GET'])
def get_aggregation_data(aggregation_name):
    """Return details for a specific aggregation."""
    print(f"get_aggregation_data called for aggregation_name: {aggregation_name}")
    
    columns = aggregations_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(aggregations_schema['table_info']['name'])} WHERE aggregation_name = :aggregation_name"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"aggregation_name": aggregation_name})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Aggregation not found"}, 404
        return jsonify(rows[0])