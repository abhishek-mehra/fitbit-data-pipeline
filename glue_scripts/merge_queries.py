# Keeping the same merge logic from the original codebase
merge_queries = {
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

def execute_merge(redshift_conn):
    """Execute merge operations for all tables"""
    with redshift_conn.cursor() as cursor:
        for query_name, query in merge_queries.items():
            try:
                print(f"Executing {query_name} merge...")
                cursor.execute(query)
                print(f"{query_name} merge completed successfully")
            except Exception as e:
                print(f"Error in {query_name} merge: {str(e)}")
                raise 