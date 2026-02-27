# Excel to MySQL Importer with Web UI

Simple Flask application that allows you to upload an Excel workbook and import its sheets into a MariaDB/MySQL database using the logic provided in your original script.

## Features

- Upload `.xlsx` or `.xls` files via web form
- Converts HTML/JSON fields to block-style JSON
- Handles PHP serialized MCQ data
- Applies foreign-key sanitization for common sheets
- Inserts/upserts records into database with batch commits
- Logs progress to `import_log.txt` and stderr/stdout

## Setup

1. **Clone workspace or copy files** to `d:\filetodb`.

2. **Create a virtual environment (optional but recommended):**
   ```powershell
   python -m venv venv
   .\venv\Scripts\Activate.ps1
   ```

3. **Install dependencies:**
   ```powershell
   pip install -r requirements.txt
   ```

4. **Database configuration** is read from environment variables or falls back to defaults in `import_utils.py`.
   You can set them before running the app:
   ```powershell
   $env:DB_HOST="localhost"
   $env:DB_USER="root"
   $env:DB_PASSWORD="root"
   $env:DB_NAME="simpointDB"
   $env:DB_PORT="3306"
   ```

5. **Run the Flask server:**
   ```powershell
   python app.py
   ```
   Navigate to [http://127.0.0.1:5000](http://127.0.0.1:5000) in your browser.

6. **Upload an Excel workbook** and click the button.  Processing results will be shown via flash messages, and details will appear in `import_log.txt`.

## Notes

- Uploaded files are stored under `uploads/` and not automatically removed.
- The app runs in debug mode by default; change `app.run(debug=True)` for production.
- Ensure the target database has the expected table structure before importing.

---

This UI provides exactly what you asked: a simple web page where you can upload an Excel file and have its data inserted into your database.