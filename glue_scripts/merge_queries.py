from typing import Dict, Any
from config import REDSHIFT_CONFIG

# Define all MERGE queries for each activity type
MERGE_QUERIES = {
    "Calories Daily": """
        MERGE INTO rural.rural_intraday_calories_daily
        USING rural.stg_rural_intraday_calories_daily
        ON rural.rural_intraday_calories_daily.participantidentifier = rural.stg_rural_intraday_calories_daily.participantidentifier
            AND rural.rural_intraday_calories_daily.date = rural.stg_rural_intraday_calories_daily.date
        REMOVE DUPLICATES;
    """,
    
    "Calories": """
        MERGE INTO rural.rural_intraday_calories
        USING rural.stg_rural_intraday_calories
        ON rural.rural_intraday_calories.participantidentifier = rural.stg_rural_intraday_calories.participantidentifier
            AND rural.rural_intraday_calories.datetime = rural.stg_rural_intraday_calories.datetime
        REMOVE DUPLICATES;
    """,
    
    "Heart Zones": """
        MERGE INTO rural.rural_intraday_heart_zones
        USING rural.stg_rural_intraday_heart_zones
        ON rural.rural_intraday_heart_zones.participantidentifier = rural.stg_rural_intraday_heart_zones.participantidentifier
            AND rural.rural_intraday_heart_zones.date = rural.stg_rural_intraday_heart_zones.date
            AND rural.rural_intraday_heart_zones.zone_name = rural.stg_rural_intraday_heart_zones.zone_name
        REMOVE DUPLICATES;
    """,
    
    # ... (similar queries for other activities)
}

def execute_merge(redshift_conn) -> Dict[str, Any]:
    """
    Execute all merge operations and return results
    Args:
        redshift_conn: Redshift connection object
    Returns:
        Dict containing execution results
    """
    results = {
        'successful_merges': [],
        'failed_merges': [],
        'errors': {}
    }
    
    with redshift_conn.cursor() as cursor:
        for query_name, query in MERGE_QUERIES.items():
            try:
                print(f"Executing {query_name} merge...")
                cursor.execute(query)
                results['successful_merges'].append(query_name)
                print(f"{query_name} merge completed successfully")
            except Exception as e:
                error_msg = f"Error in {query_name} merge: {str(e)}"
                print(error_msg)
                results['failed_merges'].append(query_name)
                results['errors'][query_name] = str(e)
        
        if not results['failed_merges']:
            redshift_conn.commit()
            print("All merges completed successfully")
        else:
            redshift_conn.rollback()
            print(f"Rolling back due to {len(results['failed_merges'])} failed merges")
    
    return results

def print_execution_summary(results: Dict[str, Any]) -> None:
    """
    Print a summary of the merge execution results
    Args:
        results: Dictionary containing execution results
    """
    print("\nExecution Summary:")
    print(f"Successful merges: {len(results['successful_merges'])}")
    print(f"Failed merges: {len(results['failed_merges'])}")
    
    if results['errors']:
        print("\nErrors encountered:")
        for query_name, error in results['errors'].items():
            print(f"{query_name}: {error}")

if __name__ == '__main__':
    # This section is for local testing only
    import redshift_conn
    
    try:
        conn = redshift_conn.RedshiftEngine()
        results = execute_merge(conn)
        print_execution_summary(results)
    except Exception as e:
        print(f"Error establishing connection: {str(e)}")
    finally:
        if 'conn' in locals():
            conn.close() 