# This file will act as configurations for the various spark jobs

def data_path():
    return '/Users/sumangangopadhyay/Downloads/nyc_taxi_data/Parking_Violations_Issued_-_Fiscal_Year_2017.csv'


def primary_response_variables():
    return 'Registration_State,Plate_Type,Violation_Code,Law_Section,Violation_Legal_Code,Issuing_Agency'


def secondary_response_variables():
    return 'Violation_County,Issuer_Squad,Vehicle_Year'


def primary_explanatory_variables():
    return 'Issue_Date,Violation_Time'
