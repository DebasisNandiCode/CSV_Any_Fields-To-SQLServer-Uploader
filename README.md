# CSV_Any_Fields-To-SQLServer-Uploader

A lightweight and configurable Python utility to upload CSV files into Microsoft SQL Server tables using SQLAlchemy and Pandas.
It automatically handles:

Column validation and mapping
Data type coercion
Text/date/numeric normalization
Logging and error handling
Environment-based configuration via .env

🚀 Features
✅ Connects to Microsoft SQL Server using credentials from .env
✅ Reads large CSV files efficiently with Pandas
✅ Automatically matches CSV columns to target table columns
✅ Discards extra/unmapped columns gracefully
✅ Converts blank/null/NaN fields intelligently
✅ Handles _Parsed timestamp fields dynamically
✅ Uploads data safely using SQLAlchemy transactions
✅ Provides detailed logging in upload.log


🧩 Folder Structure
CSV-To-SQLServer-Uploader/
│
├── upload_data.py               # Main script
├── requirements.txt             # Required dependencies
├── .env                         # Database credentials
├── upload.log                   # Auto-generated log file
└── README.md                    # Documentation


🔐 Configure .env
Create a .env file in the project root with your SQL Server credentials:
DB_SERVER=your_server_name_or_ip
DB_DATABASE=your_database_name
DB_USER=your_username
DB_PASSWORD=your_password
DB_SCHEMA=tcn


🧾 Logging
Execution logs are stored in upload.log
Each run logs:
Connection success/failure
CSV read summary
Column mapping results
Data type conversion details
Upload success/failure messages


🛠️ Dependencies
Python 3.8+
pandas
sqlalchemy
pyodbc
python-dotenv


🤝 Contributing
Pull requests are welcome!
If you find a bug or have a feature request, please open an issue or fork and submit a PR.


👤 Author
Debasis Nandi
💼 GitHub: DebasisNandiCode
📧 Contact: (debasis.webnet@gmail.com)
