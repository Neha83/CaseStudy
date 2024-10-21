from pyspark.sql.functions import col, countDistinct, desc, sum as spark_sum

def male_fatalities(df):
    """
    Analysis 1: Find the number of crashes where the number of males killed is greater than 2.
    """
    return df.filter((col('PRSN_GNDR_ID') == 'MALE') & (col('DEATH_CNT') > 2)) \
             .select('CRASH_ID') \
             .distinct() \
             .count()

def two_wheeler_crashes(df):
    """
    Analysis 2: Count the number of two-wheelers booked for crashes.
    """
    return df.filter(col('UNIT_DESC_ID').like('%MOTORCYCLE%')) \
             .select('CRASH_ID') \
             .distinct() \
             .count()

def top_vehicle_makes_airbag_deploy(df, primary_person_df):
    """
    Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in crashes in which the driver died
               and airbags did not deploy.
    """
    death_crashes = primary_person_df.filter((col('DEATH_CNT') > 0) & (col('PRSN_AIRBAG_ID') == 'NOT DEPLOYED'))
    merged_df = death_crashes.join(df, ['CRASH_ID', 'UNIT_NBR'])
    return merged_df.groupBy('VEH_MAKE_ID') \
                    .count() \
                    .orderBy(desc('count')) \
                    .limit(5)

def valid_license_hit_run(df, primary_person_df):
    """
    Analysis 4: Determine the number of vehicles with drivers having valid licenses involved in hit and run.
    """
    hit_run_df = df.filter(col('VEH_HNR_FL') == 'Y')
    valid_licenses_df = primary_person_df.filter(col('DRVR_LIC_TYPE_ID').isin('DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.'))
    return hit_run_df.join(valid_licenses_df, ['CRASH_ID', 'UNIT_NBR']).count()

def state_female_not_involved(df, primary_person_df):
    """
    Analysis 5: Which state has the highest number of accidents in which females are not involved?
    """
    no_female_df = primary_person_df.filter(col('PRSN_GNDR_ID') != 'FEMALE')
    merged_df = df.join(no_female_df, ['CRASH_ID', 'UNIT_NBR'])
    return merged_df.groupBy('VEH_LIC_STATE_ID') \
                    .count() \
                    .orderBy(desc('count')) \
                    .limit(1)

def top_3rd_to_5th_vehicle_makes_injuries(df):
    """
    Analysis 6: Determine the Top 3rd to 5th VEH_MAKE_IDs that contribute to the largest number of injuries including death.
    """
    injuries_df = df.filter((col('INCAP_INJRY_CNT') > 0) | 
                            (col('NONINCAP_INJRY_CNT') > 0) | 
                            (col('POSS_INJRY_CNT') > 0) | 
                            (col('DEATH_CNT') > 0))
    return injuries_df.groupBy('VEH_MAKE_ID') \
                      .agg(spark_sum('INCAP_INJRY_CNT').alias('total_injuries')) \
                      .orderBy(desc('total_injuries')) \
                      .limit(5) \
                      .collect()[2:5]  # Get the 3rd to 5th entries

def top_ethnic_group_by_body_style(df, primary_person_df):
    """
    Analysis 7: For all body styles involved in crashes, mention the top ethnic user group of each unique body style.
    """
    merged_df = df.join(primary_person_df, ['CRASH_ID', 'UNIT_NBR'])
    return merged_df.groupBy('VEH_BODY_STYL_ID', 'PRSN_ETHNICITY_ID') \
                    .count() \
                    .orderBy('VEH_BODY_STYL_ID', desc('count'))

def top_5_zip_codes_alcohol_contrib(df, primary_person_df):
    """
    Analysis 8: Among the crashed cars, find the Top 5 Zip Codes with the highest number of crashes where alcohol
               is a contributing factor (Use Driver Zip Code).
    """
    alcohol_related_df = df.filter(col('CONTRIB_FACTR_1_ID').like('%ALCOHOL%') |
                                   col('CONTRIB_FACTR_2_ID').like('%ALCOHOL%'))
    merged_df = alcohol_related_df.join(primary_person_df, ['CRASH_ID', 'UNIT_NBR'])
    return merged_df.groupBy('DRVR_ZIP') \
                    .count() \
                    .orderBy(desc('count')) \
                    .limit(5)

def no_damaged_property(df):
    """
    Analysis 9: Count of Distinct Crash IDs where no damaged property was observed and Damage Level (VEH_DMAG_SCL_1_ID)
               is above 4 and the car has insurance.
    """
    return df.filter((col('DAMAGED_PROPERTY').isNull()) & 
                     (col('VEH_DMAG_SCL_1_ID') > 4) & 
                     (col('FIN_RESP_TYPE_ID').isNotNull())) \
             .select('CRASH_ID') \
             .distinct() \
             .count()

def top_5_vehicle_makes_speeding(df, primary_person_df):
    """
    Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding-related offenses,
                have licensed drivers, used top 10 used vehicle colors, and are licensed in the Top 25 states
                with the highest number of offenses.
    """
    # Filter for speeding charges
    speeding_df = df.filter(col('CONTRIB_FACTR_1_ID').like('%SPEEDING%') |
                            col('CONTRIB_FACTR_2_ID').like('%SPEEDING%'))
    
    # Filter for licensed drivers
    valid_license_df = primary_person_df.filter(col('DRVR_LIC_TYPE_ID').isin('DRIVER LICENSE', 'COMMERCIAL DRIVER LIC.'))

    # Get top 10 vehicle colors
    top_10_colors_df = df.groupBy('VEH_COLOR_ID') \
                         .count() \
                         .orderBy(desc('count')) \
                         .limit(10)

    # Get top 25 states with highest offenses
    top_25_states_df = df.groupBy('VEH_LIC_STATE_ID') \
                         .count() \
                         .orderBy(desc('count')) \
                         .limit(25)

    # Join all the filtered data
    filtered_df = speeding_df.join(valid_license_df, ['CRASH_ID', 'UNIT_NBR']) \
                             .join(top_10_colors_df, ['VEH_COLOR_ID']) \
                             .join(top_25_states_df, ['VEH_LIC_STATE_ID'])

    return filtered_df.groupBy('VEH_MAKE_ID') \
                      .count() \
                      .orderBy(desc('count')) \
                      .limit(5)
