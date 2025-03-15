import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import psycopg2
from merge_queries import merge_queries  # Import from separate file

def execute_merges(redshift_conn):
    with redshift_conn.cursor() as cursor:
        for table_name, query in merge_queries.items():
            try:
                cursor.execute(query)
                print(f"Merged {table_name} successfully")
            except Exception as e:
                print(f"Error merging {table_name}: {str(e)}")
                raise

def main():
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME',
        'REDSHIFT_DATABASE',
        'REDSHIFT_USER',
        'REDSHIFT_PASSWORD',
        'REDSHIFT_HOST',
        'REDSHIFT_PORT'
    ])
    
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    # Execute merges
    with psycopg2.connect(
        dbname=args['REDSHIFT_DATABASE'],
        user=args['REDSHIFT_USER'],
        password=args['REDSHIFT_PASSWORD'],
        host=args['REDSHIFT_HOST'],
        port=args['REDSHIFT_PORT']
    ) as conn:
        execute_merges(conn)
        conn.commit()
    
    job.commit()

if __name__ == "__main__":
    main() 