import logging
import csv
from sqlalchemy import exc
from datetime import datetime
import sqlalchemy
import pandas as pd
from dateutil.relativedelta import relativedelta

def write_csv_header(file_name, header):
    with open(file_name, 'w', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(header)

def append_csv_row(file_name, row):
    with open(file_name, 'a', encoding='UTF8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(row)

def generate_sql_query(employer):
    return f"""
    WITH specs AS (
        SELECT DISTINCT el.individual_id,
            (CASE WHEN disenrollment_date IS NULL AND enrollment_date IS NOT NULL AND mapping_key = 'hcrm' THEN '1' END) AS keep
        FROM eligibilities_{employer} el
        WHERE exclude_from_enrollment = False AND (ghost IS NULL OR ghost = 'false')
        GROUP BY el.individual_id, keep
    )
    SELECT DISTINCT e.individual_id, e.enrollment_date, e.disenrollment_date, e.mapping_key
    FROM eligibilities_{employer} e
    RIGHT JOIN specs s ON s.individual_id = e.individual_id
    WHERE e.individual_id > 1 AND s.keep = '1' AND exclude_from_enrollment = False AND (ghost IS NULL OR ghost = 'false')
    ORDER BY e.individual_id;
    """

def query_database(sql_query, engine):
    try:
        return pd.read_sql(sql_query, engine)
    except exc.SQLAlchemyError as e:
        logging.error(f"Database query failed: {e}")
        return None
# individuals with mapping_key = hcrm, has another row with mapping_key != hcrm
# and hcrm disenrollment date is null
def process_individuals(base_df, employer, engine):
    exclude = ['hcrm', None]
    mapping_key_df = base_df[~base_df.mapping_key.isin(exclude)]
    if len(mapping_key_df) > 0 and pd.notna(mapping_key_df.enrollment_date).all(): # this looks at employer level for ONLY HCRM/None keys
        individuals = list(dict.fromkeys(mapping_key_df['individual_id']))
        for individual in individuals:
            ind_mask = (base_df['individual_id'] == individual)
            ind_df = base_df.loc[ind_mask]
            hcrm_mask = (ind_df['mapping_key'] == 'hcrm')
            hcrm_df = ind_df.loc[hcrm_mask] # want to look at hcrm row for one individual at a time from ind_df
            non_hcrm_mask = (mapping_key_df["individual_id"] == individual)
            non_hcrm_df = mapping_key_df.loc[non_hcrm_mask]
            disenroll_dt = (min(non_hcrm_df['enrollment_date']) - relativedelta(months=1)).strftime('%Y-%m-%d')
            hcrm_enroll = max(hcrm_df['enrollment_date']).strftime('%Y-%m-%d')
            if disenroll_dt <= hcrm_enroll: # ensure disenroll is chronologically after enrollment date
                disenroll_dt = (max(hcrm_df['enrollment_date']) + relativedelta(days=1)).strftime('%Y-%m-%d')
            
            update_query = (f""" UPDATE eligibilities_{employer}
                                SET disenrollment_date = '{disenroll_dt}' 
                                WHERE mapping_key = 'hcrm' 
                                and disenrollment_date is null
                                and individual_id = {individual} ; 
                            """)
            try:
                engine.execute(update_query)
                update_time = datetime.today().strftime("%Y/%m/%d, %H:%M:%S")
                append_csv_row('Successfully_Updated.csv', [employer, individual, disenroll_dt, update_time, 'hcrm'])
                # Log success or write to success CSV
            except exc.SQLAlchemyError as e:
                logging.error(f"Update query failed: {e}")
                append_csv_row('No_Update_Made.csv', [employer, individual, str(e)])
                # Log failure or write to failure CSV

def disenroll_hcrm_eligs(employer_ids, engine):
    header_success = ['employer_id', 'individual_id', 'disenrollment_date', 'update_time', 'mapping_key']
    header_fail = ['employer_id', 'individual_id', 'message']
    
    write_csv_header('Successfully_Updated.csv', header_success)
    write_csv_header('No_Update_Made.csv', header_fail)
    
    for employer in employer_ids:
        sql_query = generate_sql_query(employer)
        base_df = query_database(sql_query, engine)
        if base_df is not None: # if SQL query returns nothing, no individuals to disenroll
            if len(base_df) > 0:
                process_individuals(base_df, employer, engine)
            else:
                logging.info(f"No individuals to disenroll for employer: {employer}")
        else:
            logging.error(f"Failed to retrieve data for employer: {employer}")


if __name__ == "__main__":
    engine = sqlalchemy.create_engine('your_database_connection_string')
    employer_ids = ['101', '102']  # Replace with actual employer IDs
    disenroll_hcrm_eligs(employer_ids, engine)