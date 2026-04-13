# blueprints/aggregations.py
from flask import Blueprint, jsonify
from sqlalchemy import text
from sqlalchemy.engine import Result
from db import get_engine
import csv, io
from flask import Response

# Create a blueprint for aggregations
aggregations_bp = Blueprint('aggregations', __name__)

@aggregations_bp.route('/v1/aggregations', methods=['GET'])
def get_all_aggregations():
    """Return a list of all aggregations."""
    print("get_all_aggregations called")
    
    query = """
        SELECT aggregationID, aggregation_name, aggregation_description
        FROM db25_4_2_aggregations
    """
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query))
        aggregation_rows = [dict(row._mapping) for row in result]

    # Create a CSV response
    output = io.StringIO()
    writer = csv.writer(output)

    # Create header based on the fields we want to include
    header = ['aggregationID', 'aggregation_name', 'aggregation_description']
    writer.writerow(header)  # Write header

    # Write rows to CSV
    for row in aggregation_rows:
        writer.writerow([row['aggregationID'], row['aggregation_name'], row['aggregation_description']])

    # Prepare the response
    output.seek(0)  # Move to the beginning of the StringIO object
    return Response(output.getvalue(), mimetype='text/csv', headers={"Content-Disposition": "attachment;filename=all_aggregations.csv"})

@aggregations_bp.route('/v1/aggregations/<int:aggregation_id>', methods=['GET'])
def get_aggregation_data(aggregation_id):
    """Return details for a specific aggregation."""
    print(f"get_aggregation_data called for aggregation_id: {aggregation_id}")
    
    query = """
        SELECT aggregationID, aggregation_name, aggregation_description
        FROM db25_4_2_aggregations
        WHERE aggregationID = :aggregation_id
    """
    
    engine = get_engine()
    with engine.connect() as conn:
        result: Result = conn.execute(text(query), {"aggregation_id": aggregation_id})
        rows = [dict(row._mapping) for row in result]
        if not rows:
            return {"message": "Aggregation not found"}, 404
        return jsonify(rows[0])