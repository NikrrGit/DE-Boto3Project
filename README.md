# DE-Boto3Project

A data engineering pipeline that generates, processes, and analyzes sales data using AWS, PySpark, and MySQL.

## Setup Instructions

1. Clone the repository:
```bash
git clone https://github.com/NikrrGit/DE-Boto3Project.git
cd DE-Boto3Project
```

2. Create and activate virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your actual credentials
```

5. Download required JAR files:
```bash
chmod +x download_jars.sh
./download_jars.sh
```

6. Run the pipeline:
```bash
python src/main/main.py
```

## Security Notes

- Never commit `.env` file to git
- Rotate AWS credentials regularly
- Use strong passwords for MySQL
- Keep dependencies updated
- Review AWS IAM permissions regularly

## Project Structure

- `src/main/`: Main source code
  - `generate_sales_data.py`: Data generation and S3 upload
  - `process_sales_data.py`: PySpark processing
  - `main.py`: Pipeline orchestration
- `jars/`: Required JAR files
- `.env.example`: Template for environment variables

## Dependencies

- Python 3.x
- AWS Boto3
- PySpark
- MySQL Connector
- Required JAR files (downloaded automatically)