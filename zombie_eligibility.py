from sqlalchemy import exc
import sqlalchemy
import pandas as pd
from dateutil.relativedelta import relativedelta
import logging
import csv
from datetime import datetime


def disenroll_hcrm_eligs(employer_id):
    header_success = ['employer_id', 'individual_id', 'disenrollment_date', 'datetime update occurred', 'mapping_key']
    header_fail = ['employer_id', 'individual_id', 'message' ]
    with open('DATA3433_SuccessfullyUpdated.csv', 'w', encoding='UTF8', newline='') as s_file:
        writer = csv.writer(s_file)
        writer.writerow(header_success)
    with open('DATA3433_NoUpdateMade.csv', 'w', encoding='UTF8', newline='') as f_file:
        writer = csv.writer(f_file)
        writer.writerow(header_fail)
    print(employer_id)
    for employer in employer_id:
    # I only want to look at individuals where mapping key = hcrm and disenrollment is null in a population
        sql_query = (f"""with specs as 
                                (select distinct individual_id 
                                , (case when disenrollment_date is null and enrollment_date is not null and mapping_key = 'hcrm' then '1' end) as keep
                                from eligibilities_{employer} 
                                where exclude_from_enrollment = False and (ghost is null or ghost = 'false')
                                group by 1, 2
                                )
                        select distinct e.individual_id, e.enrollment_date, e.disenrollment_date, e.mapping_key
                            from eligibilities_{employer} e
                            right join specs s on s.individual_id = e.individual_id
                            where e.individual_id > 1 and s.keep = '1' and exclude_from_enrollment = False and (ghost is null or ghost = 'false')
                            order by individual_id;
                                    """)    
        try:
            base_df = pd.read_sql(sql_query, engine)
        except exc.SQLAlchemyError or sqlalchemy.exc as e:
            print(f"input dataframe has errored, {employer}. {e} ")
            logging.exception(f'an exception occured, {e}. employer: {employer}')
            continue
        if len(base_df) > 0: # if SQL query returns nothing, no individuals to disenroll and employer is good
            exclude = ['hcrm', None]
            mapping_key_df = base_df[~base_df.mapping_key.isin(exclude)] 
            if len(mapping_key_df) > 0 and pd.notna(mapping_key_df.enrollment_date).all(): # this looks at employer level for ONLY HCRM/None keys
                individuals = list(dict.fromkeys(mapping_key_df['individual_id']))
                for individual in individuals:
                    #pdb.set_trace()
                    ind_mask = (base_df['individual_id'] == individual) 
                    ind_df = base_df.loc[ind_mask] # only want to look at one individual at a time from base df. mapping key df sanitizes only HCRM keys
                    hcrm_mask = (ind_df['mapping_key'] == 'hcrm')
                    hcrm_df = ind_df.loc[hcrm_mask] # want to look at hcrm row for one individual at a time from ind_df
                    non_hcrm_mask = (mapping_key_df["individual_id"] == individual) #this ensures that we are NOT looking at HCRM or None mapping keys 
                    non_hcrm_df = mapping_key_df.loc[non_hcrm_mask]
                    disenroll_dt = (min(non_hcrm_df['enrollment_date']) - relativedelta(months=1)).strftime('%Y-%m-%d')
                    hcrm_enroll = max(hcrm_df['enrollment_date']).strftime('%Y-%m-%d') 
                    # ensure disenroll is chronologically after enrollment date
                    if disenroll_dt <= hcrm_enroll:
                        disenroll_dt = (max(hcrm_df['enrollment_date']) + relativedelta(days=1)).strftime('%Y-%m-%d')
                    
                    update_query = (f""" UPDATE eligibilities_{employer}
                                        SET disenrollment_date = '{disenroll_dt}' 
                                        WHERE mapping_key = 'hcrm' 
                                        and disenrollment_date is null
                                        and individual_id = {individual} ; 
                                    """)
                    try: 
                        print(update_query)
                        engine.autocommit = True
                        engine.execute(update_query)
                        now = datetime.today().strftime("%Y/%m/%d, %H:%M:%S")
                        mapping_key = (hcrm_df.mapping_key).to_string(index=False)
                        data = [f"{employer}", f"{individual}", f"{disenroll_dt}", f"{now}", f"{mapping_key}"]
                        with open('DATA3433_SuccessfullyUpdated.csv', 'a') as apend_file:
                            writer = csv.writer(apend_file, delimiter=',')
                            writer.writerow(data)
                            print(f'Success {employer}: {individual}')
                    except exc.SqlAlchemyError as e:
                        print(f'an error occured with {employer}; {individual}. {e}')
                        logging.exception(f"Update query failed with: {e}, employer: {employer}, individual: {individual}")
                        continue

            else:
                data = [f'{employer}', 'null', 'Only HCRM or Null mapping keys available OR no enrollment dates for DDC mapping key' ]
                with open('DATA3433_NoUpdateMade.csv', 'a') as apend_file:
                    writer = csv.writer(apend_file, delimiter=',')
                    writer.writerow(data)
                    print(f'Only HCRM or Null mapping keys available: {employer}')

        else:
            data = [f'{employer}', 'null', 'No null HCRM disenroll date for any individual.' ]
            with open('DATA3433_NoUpdateMade.csv', 'a') as apend_file:
                writer = csv.writer(apend_file, delimiter=',')
                writer.writerow(data)
                print(f'No null HCRM disenroll date for any individual: {employer}')


if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG
        , filename='DATA3433.log'
        , filemode='w'
        , format='%(name)s - %(levelname)s - %(asctime)s - %(message)s')

    # input is comma separated list of employer ids
    disenroll_hcrm_eligs([107,122,145,167,198,281,328,369])
