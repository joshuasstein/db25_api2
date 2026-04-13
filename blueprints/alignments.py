# blueprints/alignments.py
import json
import os
from flask import Blueprint, jsonify, Response
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
import config
from config import jsonpath
import csv, io

def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix."""
    return f"{base_name}{config.SUFFIX}"

# Create a blueprint for alignments
alignments_bp = Blueprint('alignments', __name__)

# Load the JSON schema
with open(jsonpath / 'alignments.json') as f:
    alignments_schema = json.load(f)

@alignments_bp.route('/v1/alignments', methods=['GET'])
def get_all_alignments():
    """Return a list of all alignments."""
    print("get_all_alignments called")
    
    columns = alignments_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    print("Suffix (inside) = " , config.SUFFIX)
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(alignments_schema['table_info']['name'])}"

    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        alignment_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    writer.writerow(column_names)  # Write header

    # Write rows to CSV
    for row in alignment_rows:
        writer.writerow([row[col] for col in column_names])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_alignments.csv"})

@alignments_bp.route('/v1/alignments/<string:alignment_name>', methods=['GET'])
def get_alignment_data(alignment_name):
    """Return details for a specific alignment."""
    print(f"get_alignment_data called for alignment_name: {alignment_name}")
    
    columns = alignments_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    
    # Use the correct column name for the WHERE clause
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(alignments_schema['table_info']['name'])} WHERE alignment_name = :alignment_name"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"alignment_name": alignment_name})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Alignment not found"}, 404
        return jsonify(rows[0])