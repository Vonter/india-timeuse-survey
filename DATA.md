# india-timeuse-survey

The source microdata is available on the [MoSPI Microdata website](https://microdata.gov.in/NADA/index.php/catalog/236). Mirrors of the microdata are also available on OpenCity:
- [Household Data in CSV Format](https://data.opencity.in/dataset/national-time-use-survey-2024/resource/household-data-in-csv-format)
- [Personal Data in CSV Format](https://data.opencity.in/dataset/national-time-use-survey-2024/resource/personal-data-in-csv-format)

## Data dictionary

| Variable | Type | Description |
|----------|------|-------------|
| `person_id` | string | Unique identifier for an individual person, created by combining various identifiers from the survey |
| `state` | string | Name of the state where the respondent lives |
| `district` | string | Name of the district where the respondent lives |
| `gender` | string | Gender of the person (male, female, transgender) |
| `age` | integer | Age of the person in years |
| `marital_status` | string | Marital status (never married, currently married, widowed, divorced/separated, etc.) |
| `education` | string | Highest level of education attained by the person |
| `religion` | string | Religious affiliation of the person |
| `social_group` | string | Social grouping (Scheduled Tribe, Scheduled Caste, Other Backward Class, Others) |
| `household_size` | integer | Number of people living in the household |
| `monthly_expenditure` | float | Usual monthly consumer expenditure |
| `principal_activity` | string | Primary employment/activity status of the person |
| `industry` | string | Industry in which the person works (based on 2-digit NIC 2008 code) |
| `activity_serial_no` | integer | Serial number of the activity recorded during the day |
| `time_from` | string | Start time of the activity (HH:MM format) |
| `time_to` | string | End time of the activity (HH:MM format) |
| `multiple_activity` | integer | Indicator if multiple activities were performed in the time slot (0/1) |
| `simultaneous_activity` | integer | Indicator if this was a simultaneous activity (0/1) |
| `is_major_activity` | integer | Indicator if this was a major activity (0/1) |
| `activity_code` | string | Detailed description of the activity (mapped from 3-digit activity code) |
| `activity_location` | string | Location where the activity was performed (home, fixed location, non-fixed location, etc.) |
| `payment_status` | string | Whether the activity was paid or unpaid |
| `enterprise_type` | string | Type of enterprise where the activity was performed (proprietary, partnership, government, etc.) |
| `day_of_week` | string | Day of the week when the activity was recorded |
| `day_type` | string | Type of day (normal day, other day) |
| `relation_to_head` | string | Relationship of the person to the head of household |
| `response_code` | string | Response code for survey quality control |

## Mapping from code to name

The source microdata uses codes for various fields. The mappings for code to human readable name are taken from the following documents:

- District code: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/6a7ad168-aab2-466e-96be-2715dd338ff7/view/b603b92e-0b2b-49e6-a5a6-6ffec0f6da1c (Page 102-126)
- Education: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/6a7ad168-aab2-466e-96be-2715dd338ff7/view/b603b92e-0b2b-49e6-a5a6-6ffec0f6da1c (Page 43)
- Religion: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/d9a26d5b-ceba-4e9f-93cd-348ae7b5cf21/view/b1df2615-6b08-4ff4-b5b7-0c65b459f44b (Page 13)
- Social group: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/d9a26d5b-ceba-4e9f-93cd-348ae7b5cf21/view/b1df2615-6b08-4ff4-b5b7-0c65b459f44b (Page 13)
- Marital status: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/d9a26d5b-ceba-4e9f-93cd-348ae7b5cf21/view/b1df2615-6b08-4ff4-b5b7-0c65b459f44b (Page 13)
- Gender: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/d9a26d5b-ceba-4e9f-93cd-348ae7b5cf21/view/b1df2615-6b08-4ff4-b5b7-0c65b459f44b (Page 12)
- Enterprise type: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/d9a26d5b-ceba-4e9f-93cd-348ae7b5cf21/view/b1df2615-6b08-4ff4-b5b7-0c65b459f44b (Page 18)
- Activity location: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/d9a26d5b-ceba-4e9f-93cd-348ae7b5cf21/view/b1df2615-6b08-4ff4-b5b7-0c65b459f44b (Page 18)
- Activity code: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/6a7ad168-aab2-466e-96be-2715dd338ff7/view/b603b92e-0b2b-49e6-a5a6-6ffec0f6da1c (Page 79-85)
- Principal activity: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/d9a26d5b-ceba-4e9f-93cd-348ae7b5cf21/view/b1df2615-6b08-4ff4-b5b7-0c65b459f44b (Page 13)
- Payment status: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/d9a26d5b-ceba-4e9f-93cd-348ae7b5cf21/view/b1df2615-6b08-4ff4-b5b7-0c65b459f44b (Page 18)
- Industry code: https://www.ncs.gov.in/Documents/NIC_Sector.pdf
- Relation to head: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/d9a26d5b-ceba-4e9f-93cd-348ae7b5cf21/view/b1df2615-6b08-4ff4-b5b7-0c65b459f44b (Page 13)
- Day of week: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/d9a26d5b-ceba-4e9f-93cd-348ae7b5cf21/view/b1df2615-6b08-4ff4-b5b7-0c65b459f44b (Page 15)
- Day type: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/d9a26d5b-ceba-4e9f-93cd-348ae7b5cf21/view/b1df2615-6b08-4ff4-b5b7-0c65b459f44b (Page 15)
- Response code: https://data.opencity.in/dataset/national-time-use-survey-2024/resource/d9a26d5b-ceba-4e9f-93cd-348ae7b5cf21/view/b1df2615-6b08-4ff4-b5b7-0c65b459f44b (Page 15)

The full list of mappings are stored in the [mappings.py](mappings.py) file.