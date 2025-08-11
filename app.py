from flask import Flask, request, jsonify, send_file, render_template_string
import pandas as pd
import mysql.connector
import os

app = Flask(__name__)

# MySQL DB config
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "csvflask"
}


# Required fields & rules
REQUIRED_FIELDS = ["user", "role", "street", "city", "country1", "pincode", "state", "country2"]

def validate_row(row):
    """
    Validate one row of CSV.
    Returns a list of error messages.
    """
    errors = []

    # 1. Check missing/empty values
    for field in REQUIRED_FIELDS:
        if pd.isna(row[field]) or str(row[field]).strip() == "":
            errors.append(f"{field} is missing")

    # 2. Check data types
    if pd.notna(row.get("pincode")) and not str(row["pincode"]).isdigit():
        errors.append("Pincode must be numeric")

    return errors


@app.route('/upload_csv', methods=['GET', 'POST'])

def upload_csv():
    if request.method == 'GET':
        return '''
        <!doctype html>
        <html>
        <body>
            <h2>Upload CSV File</h2>
            <form method="post" enctype="multipart/form-data">
                <input type="file" name="file" required>
                <input type="submit" value="Upload">
            </form>
        </body>
        </html>
        '''
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']
    if file.filename.strip() == '':
        return jsonify({"error": "Empty filename"}), 400

    try:
        df = pd.read_csv(file)
    except Exception as e:
        return jsonify({"error": f"Invalid CSV file: {str(e)}"}), 400

    valid_rows = []
    invalid_rows = []

    # Validate every row
    for _, row in df.iterrows():
        errors = validate_row(row)
        if errors:
            row_data = row.to_dict()
            row_data["error_reason"] = "; ".join(errors)
            invalid_rows.append(row_data)
        else:
            valid_rows.append(row.to_dict())

    # Insert valid rows into MySQL
    if valid_rows:
        try:
            conn = mysql.connector.connect(**DB_CONFIG)
            cursor = conn.cursor()
            insert_query = """
                INSERT INTO users (user, role, street, city, country1, pincode, state, country2)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            for row in valid_rows:
                values = tuple(row[col] if pd.notna(row[col]) else None for col in REQUIRED_FIELDS)
                cursor.execute(insert_query, values)
            conn.commit()
            cursor.close()
            conn.close()
        except mysql.connector.Error as e:
            return jsonify({"error": f"MySQL Error: {str(e)}"}), 500

    # Save invalid rows to Excel
    error_file_path = "invalid_rows.xlsx"
    if invalid_rows:
        pd.DataFrame(invalid_rows).to_excel(error_file_path, index=False)

    return jsonify({
        "inserted_count": len(valid_rows),
        "failed_count": len(invalid_rows),
        "error_file": error_file_path if invalid_rows else None
    })


@app.route('/download_errors', methods=['GET'])
def download_errors():
    error_file_path = "invalid_rows.xlsx"
    if os.path.exists(error_file_path):
        return send_file(error_file_path, as_attachment=True)
    return jsonify({"error": "No error file found"}), 404


@app.route('/get_users', methods=['GET'])
def get_users():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)  # dictionary=True returns JSON-friendly dicts
        cursor.execute("SELECT * FROM users")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        return jsonify(rows)  # returns all rows as JSON
    except mysql.connector.Error as err:
        return jsonify({"error": f"MySQL Error: {err}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
