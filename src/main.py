import sys
from scripts.etl import load_config, create_spark_session, load_data
from scripts.analysis import male_fatalities, two_wheeler_crashes,top_vehicle_makes_airbag_deploy, 
                              valid_license_hit_run, state_female_not_involved, top_3rd_to_5th_vehicle_makes_injuries, 
                              top_ethnic_group_by_body_style, top_5_zip_codes_alcohol_contrib, 
                              no_damaged_property, top_5_vehicle_makes_speeding

def run_analysis(config_path):
    config = load_config(config_path)
    
    # Create Spark session
    spark = create_spark_session(config['spark']['app_name'], config['spark']['master'])
    
    # Load data
    df_dict = load_data(spark, config)

    # Perform analyses
    logger.info("Starting analyses...")

    # Analysis 1
    male_fatalities_count = male_fatalities(df_dict['primary_person'])
    logger.info(f"Number of crashes where more than 2 males were killed: {male_fatalities_count}")

    # Analysis 2
    two_wheeler_crashes_count = two_wheeler_crashes(df_dict['units'])
    logger.info(f"Number of two-wheelers booked for crashes: {two_wheeler_crashes_count}")

    # Analysis 3
    top_5_vehicle_makes = top_vehicle_makes_airbag_deploy(df_dict['units'], df_dict['primary_person'])
    logger.info(f"Top 5 Vehicle Makes in crashes with driver death and undeployed airbags: {top_5_vehicle_makes.show()}")

    # Analysis 4
    valid_license_hit_run_count = valid_license_hit_run(df_dict['units'], df_dict['primary_person'])
    logger.info(f"Number of vehicles with valid license drivers involved in hit and run: {valid_license_hit_run_count}")

    # Analysis 5
    top_state = state_female_not_involved(df_dict['units'], df_dict['primary_person'])
    logger.info(f"State with the highest number of crashes without female involvement: {top_state.show()}")

    # Analysis 6
    top_3rd_5th_vehicle_makes = top_3rd_to_5th_vehicle_makes_injuries(df_dict['units'])
    logger.info(f"Top 3rd to 5th Vehicle Makes contributing to injuries: {top_3rd_5th_vehicle_makes}")

    # Analysis 7
    top_ethnic_group_by_body_style_df = top_ethnic_group_by_body_style(df_dict['units'], df_dict['primary_person'])
    logger.info(f"Top ethnic group by body style: {top_ethnic_group_by_body_style_df.show()}")

    # Analysis 8
    top_5_zip_codes = top_5_zip_codes_alcohol_contrib(df_dict['units'], df_dict['primary_person'])
    logger.info(f"Top 5 zip codes for alcohol-related crashes: {top_5_zip_codes.show()}")

    # Analysis 9
    no_damage_crash_count = no_damaged_property(df_dict['damages'])
    logger.info(f"Count of distinct crash IDs with no damaged property and high damage levels: {no_damage_crash_count}")

    # Analysis 10
    top_5_vehicle_makes_speeding_df = top_5_vehicle_makes_speeding(df_dict['units'], df_dict['primary_person'])
    logger.info(f"Top 5 vehicle makes for speeding-related offenses: {top_5_vehicle_makes_speeding_df.show()}")

    # Stop the Spark session
    spark.stop()
    logger.info("Spark session stopped.")

if __name__ == "__main__":
    # Command-line arguments
    parser = argparse.ArgumentParser(description='Crash Data Analysis Application')
    parser.add_argument('--config', required=True, help='Path to the configuration YAML file')
    
    args = parser.parse_args()

    # Run the main function
    main(args.config)
