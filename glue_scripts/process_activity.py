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