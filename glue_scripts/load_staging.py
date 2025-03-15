import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def load_to_staging(glueContext, source_path, table_name):
    # Read parquet files
    dynamic_frame = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [source_path]},
        format="parquet"
    )
    
    # Load to Redshift staging
    glueContext.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection="redshift_connection",
        connection_options={
            "dbtable": f"rural.stg_{table_name}",
            "database": "your_database"
        },
        redshift_tmp_dir="s3://your-temp-bucket/temp/"
    )

def main():
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Load each activity type to staging
    activities = ['calories', 'distance', 'elevation', 'floors', 'heart', 'steps']
    for activity in activities:
        load_to_staging(
            glueContext,
            f"s3://your-bucket/processed/{activity}",
            f"rural_intraday_{activity}"
        )
    
    job.commit()

if __name__ == "__main__":
    main() 