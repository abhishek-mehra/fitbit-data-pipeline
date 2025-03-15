from datetime import datetime
import boto3
import json

class MetadataHandler:
    def __init__(self, activity_type):
        self.activity_type = activity_type
        self.metadata = {
            'metadata_file': {},
            'processing_stats': {
                'total_files': 0,
                'duplicates': 0,
                'zero_values': 0,
                'processed': 0
            }
        }
    
    def is_processed(self, participant_id: str, date_str: str) -> bool:
        """Check if a specific record has been processed"""
        if not date_str:
            return False
            
        try:
            year, month, day = date_str.split('-')
            return self.metadata.get('metadata_file', {})\
                          .get(year, {})\
                          .get(month, {})\
                          .get(day, {})\
                          .get(self.activity_type, {})\
                          .get(participant_id, False)
        except:
            return False
    
    def record_processed(self, participant_id: str, date_str: str, file_path: str) -> None:
        """Mark a record as processed"""
        if not date_str:
            return
            
        year, month, day = date_str.split('-')
        
        # Create nested structure
        self.metadata.setdefault('metadata_file', {})
        self.metadata['metadata_file'].setdefault(year, {})
        self.metadata['metadata_file'][year].setdefault(month, {})
        self.metadata['metadata_file'][year][month].setdefault(day, {})
        self.metadata['metadata_file'][year][month][day].setdefault(self.activity_type, {})
        
        # Mark as processed
        self.metadata['metadata_file'][year][month][day][self.activity_type][participant_id] = True
    
    def update_stats(self, stat_type: str):
        """Update processing statistics"""
        if stat_type in self.metadata['processing_stats']:
            self.metadata['processing_stats'][stat_type] += 1
    
    def get_stats(self):
        """Get current processing statistics"""
        return self.metadata['processing_stats']
    
    def save_to_s3(self, bucket: str, prefix: str):
        """Save metadata to S3 for debugging/tracking"""
        s3 = boto3.client('s3')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        key = f"{prefix}/metadata/{self.activity_type}_{timestamp}.json"
        
        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(self.metadata)
        ) 