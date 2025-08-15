import cx_Oracle
import pandas as pd

def get_full_lineage_single_query(conn, target_table):
    """
    Recursively fetch full lineage for all fields of a target table
    without multiple DB hits.
    """
    # 1️⃣ Pull the whole mapping table into memory
    mapping_df = pd.read_sql("SELECT SOURCE_FIELD, TARGET_FIELD FROM MAP_SPEC", conn)
    
    # 2️⃣ Build an index: target_field -> list of source_fields
    mapping_dict = mapping_df.groupby("TARGET_FIELD")["SOURCE_FIELD"].apply(list).to_dict()
    
    lineage_records = []
    visited_tables = set()
    
    def recursive_trace(table, level=0):
        """Internal recursive function"""
        for target_field, source_list in mapping_dict.items():
            if target_field.startswith(f"{table}."):
                for source_field in source_list:
                    lineage_records.append({
                        "TARGET_FIELD": target_field,
                        "SOURCE_FIELD": source_field,
                        "LEVEL": level
                    })
                    
                    # Recurse if source is table.column
                    if "." in source_field:
                        source_table = source_field.split(".")[0]
                        if source_table not in visited_tables:
                            visited_tables.add(source_table)
                            recursive_trace(source_table, level + 1)
    
    # Start recursion from the given table
    visited_tables.add(target_table)
    recursive_trace(target_table)
    
    lineage_df = pd.DataFrame(lineage_records)
    upstream_dict = lineage_df.groupby("TARGET_FIELD")["SOURCE_FIELD"].apply(list).to_dict()
    
    return lineage_df, upstream_dict

# ------------------- Usage Example -------------------
conn = cx_Oracle.connect("user/password@dsn")

df_lineage, dict_lineage = get_full_lineage_single_query(conn, "CLIENT")

print(df_lineage)
print(dict_lineage)


import cx_Oracle
import pandas as pd

def get_full_lineage(conn, target_table):
    """
    Recursively fetch full lineage for all fields of a target table.
    
    Args:
        conn: cx_Oracle connection object
        target_table: str, name of the target table (e.g., 'CLIENT')
    
    Returns:
        pd.DataFrame: columns ['TARGET_FIELD', 'SOURCE_FIELD', 'LEVEL']
        dict: mapping target_field -> list of all upstream source fields
    """
    
    lineage_records = []
    visited_targets = set()  # Avoid infinite loops
    
    def recursive_fetch(table, level=0):
        """
        Internal recursive function
        """
        query = f"""
            SELECT SOURCE_FIELD, TARGET_FIELD
            FROM MAP_SPEC
            WHERE TARGET_FIELD LIKE '{table}.%';
        """
        df = pd.read_sql(query, conn)
        
        if df.empty:
            return
        
        for _, row in df.iterrows():
            target_field = row['TARGET_FIELD']
            source_field = row['SOURCE_FIELD']
            
            lineage_records.append({
                'TARGET_FIELD': target_field,
                'SOURCE_FIELD': source_field,
                'LEVEL': level
            })
            
            # Extract source table if source_field is in table.column format
            if '.' in source_field:
                source_table = source_field.split('.')[0]
                # Recurse only if we haven’t visited this target table already
                if source_table not in visited_targets:
                    visited_targets.add(source_table)
                    recursive_fetch(source_table, level + 1)
    
    # Start recursion
    visited_targets.add(target_table)
    recursive_fetch(target_table, level=0)
    
    # Convert to DataFrame
    lineage_df = pd.DataFrame(lineage_records)
    
    # Build dictionary: target_field -> list of all upstream sources
    upstream_dict = lineage_df.groupby('TARGET_FIELD')['SOURCE_FIELD'].apply(list).to_dict()
    
    return lineage_df, upstream_dict

# ------------------- Usage Example -------------------
conn = cx_Oracle.connect("user/password@dsn")

df_lineage, dict_lineage = get_full_lineage(conn, "CLIENT")

print(df_lineage)
print(dict_lineage)