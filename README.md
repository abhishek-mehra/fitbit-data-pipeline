# Fitbit Intraday Data Processing Pipeline

A cloud-based data processing pipeline that extracts Fitbit intraday activity data from S3, transforms it into Parquet format, and loads it into Amazon Redshift using AWS Glue and Apache Airflow.

## Architecture Overview

![Architecture Diagram](./docs/assets/architecture.png)

The pipeline consists of four main components:

1. **Data Processing**
   - Parallel processing of 6 activity types:
     - Heart Rate
     - Calories
     - Steps
     - Elevation
     - Distance
     - Floors
   - JSON to Parquet conversion
   - Staging and Production data layers

2. **Orchestration**
   - Apache Airflow for workflow management
   - Weekly scheduled execution
   - Task dependency management

3. **Data Flow**
   - Source: S3 (Fitbit JSON Data)
   - Processing: AWS Glue ETL
   - Destination: Amazon Redshift

4. **Error Handling**
   - Retry mechanisms
   - Data validation checks
   - Error notifications

## Data Processing Details

This pipeline handles the following types of Fitbit activity data:
- Heart rate and heart rate zones 
- Calories burned
- Steps taken
- Elevation
- Distance traveled
- Floors climbed

Data is processed at both daily summary and intraday (minute-by-minute) levels.

## Prerequisites

- AWS Account with access to:
  - S3 buckets (source and destination)
  - AWS Glue
  - Amazon Redshift
  - IAM roles and permissions
- Apache Airflow environment
- Python 3.6+

## Key Features

- Parallel activity processing
- In-memory metadata tracking
- Two-stage loading (staging to production)
- Automated error handling and retries
- Data validation and quality checks

## Configuration

The pipeline uses environment variables for configuration. Key configurations include:
- S3 bucket locations
- Redshift connection details
- Processing schedule
- IAM roles and permissions

## Error Handling

The pipeline includes comprehensive error handling:
- Automatic retries for failed tasks
- Data validation checks
- Error notifications
- Processing statistics tracking

## Monitoring

Monitor the pipeline through:
- Airflow web interface
- AWS CloudWatch logs
- Processing statistics
- Error notifications

## Contributing

Please refer to CONTRIBUTING.md for guidelines on contributing to this project.

## License

[Your License Information]


