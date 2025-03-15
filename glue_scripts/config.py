import os
from typing import Dict, Any

# S3 Configuration
S3_CONFIG = {
    'SOURCE_BUCKET': os.getenv('FITBIT_SOURCE_BUCKET', 'umms-research-radx-image-processing'),
    'SOURCE_PREFIX': os.getenv('FITBIT_SOURCE_PREFIX', 'Onboarding/RADx/RURAL/fitbit_intraday'),
    'DEST_BUCKET': os.getenv('FITBIT_DEST_BUCKET', 'umasschan-plum-data-lab'),
    'DEST_PREFIX': os.getenv('FITBIT_DEST_PREFIX', 'Rural/intraday'),
    'GLUE_SCRIPTS_BUCKET': os.getenv('GLUE_SCRIPTS_BUCKET', 'your-glue-scripts-bucket'),
}

# Redshift Configuration
REDSHIFT_CONFIG = {
    'TEMP_DIR': os.getenv('REDSHIFT_TEMP_DIR', 's3://your-temp-bucket/temp/'),
    'DATABASE': os.getenv('REDSHIFT_DATABASE', 'your_database'),
    'SCHEMA': os.getenv('REDSHIFT_SCHEMA', 'rural'),
}

# AWS Configuration
AWS_CONFIG = {
    'REGION': os.getenv('AWS_REGION', 'us-east-1'),
    'GLUE_JOB_PREFIX': os.getenv('GLUE_JOB_PREFIX', 'rural_fitbit'),
}

def get_glue_job_params(activity_type: str = None) -> Dict[str, Any]:
    """Get parameters for Glue job configuration"""
    params = {
        '--source_bucket': S3_CONFIG['SOURCE_BUCKET'],
        '--source_prefix': S3_CONFIG['SOURCE_PREFIX'],
        '--dest_bucket': S3_CONFIG['DEST_BUCKET'],
        '--dest_prefix': S3_CONFIG['DEST_PREFIX'],
    }
    
    if activity_type:
        params['--activity_type'] = activity_type
        
    return params

def get_redshift_params() -> Dict[str, str]:
    """Get Redshift connection parameters from Airflow connection"""
    return {
        '--REDSHIFT_DATABASE': '{{ conn.redshift_default.schema }}',
        '--REDSHIFT_USER': '{{ conn.redshift_default.login }}',
        '--REDSHIFT_PASSWORD': '{{ conn.redshift_default.password }}',
        '--REDSHIFT_HOST': '{{ conn.redshift_default.host }}',
        '--REDSHIFT_PORT': '{{ conn.redshift_default.port }}',
        '--REDSHIFT_SCHEMA': REDSHIFT_CONFIG['SCHEMA'],
        '--REDSHIFT_TEMP_DIR': REDSHIFT_CONFIG['TEMP_DIR'],
    } 