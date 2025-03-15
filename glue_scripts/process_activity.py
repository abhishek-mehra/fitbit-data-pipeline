import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
from datetime import datetime
import json
from schemas import activity_schemas, schema_mapping
from metadata_handler import MetadataHandler
import re
from typing import List, Tuple, Dict, Any

def process_activity_files(glueContext, activity_type, source_bucket, source_prefix):
    metadata_handler = MetadataHandler(activity_type)
    
    # Process files
    for file in list_files(source_bucket, source_prefix):
        participant_id = extract_participant_id(file)
        date_str = extract_date(file)
        
        # Check if already processed
        if metadata_handler.is_processed(participant_id, date_str):
            metadata_handler.update_stats('duplicates')
            continue
            
        # Process file
        try:
            process_file(file, schema_mapping[activity_type])
            metadata_handler.record_processed(participant_id, date_str, file)
            metadata_handler.update_stats('processed')
        except Exception as e:
            print(f"Error processing file: {str(e)}")
    
    # Save metadata to S3 for tracking
    metadata_handler.save_to_s3(source_bucket, source_prefix)
    
    return metadata_handler.get_stats()

def extract_activity_data(data, activity_type, participant_id):
    daily_data = []
    intraday_data = []
    
    # Extract daily summary
    daily_summary = data.get(f'activities-{activity_type}', [])
    for summary in daily_summary:
        if activity_type == 'heart':
            # Special processing for heart rate zones
            process_heart_zones(summary, participant_id, daily_data)
        else:
            # Standard processing for other activities
            process_standard_daily(summary, participant_id, activity_type, daily_data)
    
    # Extract intraday data
    intraday_key = f'activities-{activity_type}-intraday'
    if intraday_key in data:
        process_intraday_data(
            data[intraday_key].get('dataset', []),
            participant_id,
            activity_type,
            intraday_data
        )
    
    return daily_data, intraday_data

def list_files(bucket: str, prefix: str) -> List[str]:
    """
    List all JSON files in the specified S3 bucket and prefix
    """
    s3 = boto3.client('s3')
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.json'):
                    files.append(obj['Key'])
    
    return files

def extract_participant_id(file_path: str) -> str:
    """
    Extract participant ID from file path
    Expected format: path/to/PARTICIPANT_ID/date/file.json
    """
    parts = file_path.split('/')
    for part in parts:
        # Assuming participant IDs follow a specific pattern
        # Modify this regex pattern to match your participant ID format
        if re.match(r'^[A-Z0-9]{6,}$', part):
            return part
    raise ValueError(f"Could not extract participant ID from {file_path}")

def extract_date(file_path: str) -> str:
    """
    Extract date from file path
    Expected format: path/to/participant/YYYY-MM-DD/file.json
    """
    date_pattern = r'\d{4}-\d{2}-\d{2}'
    match = re.search(date_pattern, file_path)
    if match:
        return match.group(0)
    raise ValueError(f"Could not extract date from {file_path}")

def process_file(file_path: str, schemas: Dict) -> Tuple[DynamicFrame, DynamicFrame]:
    """
    Process a single JSON file and convert to daily and intraday DynamicFrames
    """
    s3 = boto3.client('s3')
    
    try:
        response = s3.get_object(Bucket=bucket, Key=file_path)
        data = json.loads(response['Body'].read().decode('utf-8'))
        
        participant_id = extract_participant_id(file_path)
        daily_data, intraday_data = extract_activity_data(data, activity_type, participant_id)
        
        # Convert to DynamicFrames using the provided schemas
        daily_frame = DynamicFrame.fromDF(
            spark.createDataFrame(daily_data, schemas['daily']),
            glueContext,
            "daily_frame"
        )
        
        intraday_frame = DynamicFrame.fromDF(
            spark.createDataFrame(intraday_data, schemas['intraday']),
            glueContext,
            "intraday_frame"
        )
        
        return daily_frame, intraday_frame
        
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        raise

def process_heart_zones(summary: Dict[str, Any], participant_id: str, daily_data: List[Dict]) -> None:
    """
    Process heart rate zones data from daily summary
    """
    date = summary.get('dateTime')
    zones = summary.get('value', {}).get('heartRateZones', [])
    
    for zone in zones:
        daily_data.append({
            'participantidentifier': participant_id,
            'date': datetime.strptime(date, '%Y-%m-%d').date(),
            'zone_name': zone.get('name'),
            'calories_out': float(zone.get('caloriesOut', 0)),
            'min_heart_rate': int(zone.get('min', 0)),
            'max_heart_rate': int(zone.get('max', 0)),
            'minutes': float(zone.get('minutes', 0))
        })

def process_standard_daily(summary: Dict[str, Any], participant_id: str, activity_type: str, daily_data: List[Dict]) -> None:
    """
    Process standard daily summary data for non-heart-rate activities
    """
    daily_data.append({
        'participantidentifier': participant_id,
        'date': datetime.strptime(summary.get('dateTime'), '%Y-%m-%d').date(),
        'value': float(summary.get('value', 0))
    })

def process_intraday_data(dataset: List[Dict], participant_id: str, activity_type: str, intraday_data: List[Dict]) -> None:
    """
    Process intraday (minute-by-minute) activity data
    """
    for record in dataset:
        time_str = record.get('time')
        date_str = record.get('date', datetime.now().strftime('%Y-%m-%d'))
        
        # Combine date and time
        datetime_str = f"{date_str} {time_str}"
        timestamp = datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
        
        intraday_record = {
            'participantidentifier': participant_id,
            'datetime': timestamp,
            'value': float(record.get('value', 0))
        }
        
        # Add additional fields for calories if applicable
        if activity_type == 'calories':
            intraday_record.update({
                'level': int(record.get('level', 0)),
                'mets': float(record.get('mets', 0))
            })
            
        intraday_data.append(intraday_record)

def main():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'activity_type',
        'source_bucket',
        'source_prefix',
        'dest_bucket',
        'dest_prefix'
    ])
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    process_activity_files(
        glueContext,
        args['activity_type'],
        args['source_bucket'],
        args['source_prefix']
    )
    
    job.commit()

if __name__ == "__main__":
    main() 