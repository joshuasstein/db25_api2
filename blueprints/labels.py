# blueprints/labels.py
from flask import Blueprint, jsonify
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
import csv, io
from flask import Response
import config
from config import jsonpath
import json

# Create a blueprint for labels
labels_bp = Blueprint('labels', __name__)

# Load the JSON schema
with open(jsonpath / 'labels.json') as f:
    labels_schema = json.load(f)

def make_filename(base_name: str) -> str:
    """Generate a filename with the global suffix."""
    return f"{base_name}{config.SUFFIX}"

@labels_bp.route('/v1/labels', methods=['GET'])
def get_all_labels():
    """Return a list of all labels."""
    print("get_all_labels called")
    
    columns = labels_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(labels_schema['table_info']['name'])}"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        label_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    writer.writerow(column_names)  # Write header

    # Write rows to CSV
    for row in label_rows:
        writer.writerow([row[col] for col in column_names])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_labels.csv"})

@labels_bp.route('/v1/labels/<string:label_name>', methods=['GET'])
def get_label_data(label_name):
    """Return details for a specific label."""
    print(f"get_label_data called for label_name: {label_name}")
    
    columns = labels_schema['table_info']['columns']
    column_names = [col for col in columns if columns[col]['keep']]
    query = f"SELECT {', '.join(column_names)} FROM {make_filename(labels_schema['table_info']['name'])} WHERE label_name = :label_name"
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"label_name": label_name})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Label not found"}, 404
        return jsonify(rows[0])