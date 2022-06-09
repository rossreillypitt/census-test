import requests
import zipfile
from io import BytesIO
import pandas as pd

sequence = 1
year = 2019
base_url = 'https://www2.census.gov/programs-surveys/acs/summary_file/'
tract_block_loc = '/Tracts_Block_Groups_Only/'
all_other_loc = '/All_Geographies_Not_Tracts_Block_Groups/'
tract_block_url = 'https://www2.census.gov/programs-surveys/acs/summary_file/2019/data/5_year_seq_by_state/Pennsylvania/Tracts_Block_Groups_Only/'
all_other_url = 'https://www2.census.gov/programs-surveys/acs/summary_file/2019/data/5_year_seq_by_state/Pennsylvania/All_Geographies_Not_Tracts_Block_Groups/'

#creates a dictionary with state postal abbreviations as keys and full state names (styled as they would be in Census URLs) as the values.
#Note: double check that all of these are as they appear in the census URLs.
state_names = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 'Delaware', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'NewHampshire', 'NewJersey', 'NewMexico', 'NewYork', 'NorthCarolina', 'NorthDakota', 'Ohio', 'Oklahoma', 'Oregon', 'Pennsylvania', 'PuertoRico', 'RhodeIsland', 'SouthCarolina', 'SouthDakota', 'Tennessee', 'Texas', 'Utah', 'Vermont', 'Virginia', 'Washington', 'WestVirginia', 'Wisconsin',  'Wyoming']
state_abbrevs = ['AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY']
state_name_abbr_dict = {}
for x in range(len(state_names)):
    state_name_abbr_dict[state_abbrevs[x].lower()] = state_names[x]

#creates dictionary with Logrecnos as keys and GEOIds as the values.
#currently contains a cheat for debugging purposes -- url/file name will need to accept user input later.  
geog_file_csv_output = pd.read_csv(f'{tract_block_url}g20195pa.csv', header=None, dtype = str)
logrecno_dict = {}
geog_dict = geog_file_csv_output.to_dict('records')
for i in range(len(geog_dict)):
    logrecno_dict[geog_dict[i][4]] = f'{geog_dict[i][48][:5]}00{geog_dict[i][48][5:]}'

#creates table of labels for sequences
zip_file_name = f'{year}_5yr_Summary_FileTemplates.zip'
zipped_file = requests.get(f'{base_url}{year}/data/{zip_file_name}')
read_file = zipfile.ZipFile(BytesIO(zipped_file.content))
seq = 1
seq_header_dF = pd.DataFrame([])
for item in read_file.namelist():
    seq_file_name = f'seq{seq}.xlsx'
    if item == '2019_SFGeoFileTemplate.xlsx':
        pass
    else:
        seq_header_txt = pd.read_excel(read_file.open(seq_file_name), header=None)
        seq_header_txt = seq_header_txt.iloc[:, 6:].transpose()
        seq_header_txt.loc[:, len(seq_header_txt.columns)]=f'{seq:04d}'
        seq_header_dF = pd.concat([seq_header_dF, seq_header_txt]).reset_index(drop=True)
        seq += 1

#This reads in the estimate and margin files for every sequence for a given state/year and combines them into one large object.
#Currently, the while statement is set to a number -- this is for debugging purposes. When the code is ready to run, it should be changed to a T/F flag.  
def acs_5yr_csv_output_all(year, st_abbv):
    def grabber(geog_type):
        flag = True
        concat_dF = pd.DataFrame([])
        sequence = 1
        while sequence < 2:
            zip_file_name = f'{year}5{st_abbv}{sequence:04d}000.zip'
            e_file_name = f'e{year}5{st_abbv}{sequence:04d}000.txt'
            m_file_name = f'm{year}5{st_abbv}{sequence:04d}000.txt'
            tract_file_check = requests.head(
                f'{base_url}{year}/data/5_year_seq_by_state/{state_name_abbr_dict[st_abbv]}{geog_type}{zip_file_name}')
            if tract_file_check.status_code == 200:
                flag = True
                zipped_file = requests.get(
                    f'{base_url}{year}/data/5_year_seq_by_state/{state_name_abbr_dict[st_abbv]}{geog_type}{zip_file_name}')
                read_file = zipfile.ZipFile(BytesIO(zipped_file.content))

                estimate_seq_txt = pd.read_csv(read_file.open(e_file_name), header=None,
                                                dtype=str)
                margin_seq_txt = pd.read_csv(read_file.open(m_file_name), header=None,
                                                dtype=str)

                estimate_and_margin = pd.concat([estimate_seq_txt, margin_seq_txt])
                concat_dF = pd.concat([concat_dF, estimate_and_margin])
                sequence += 1
            else:
                flag = False
        return concat_dF

    tract_block_dF = grabber(tract_block_loc)
    all_other_dF = grabber(all_other_loc)
    combined_dF = pd.concat([tract_block_dF, all_other_dF]).reset_index(drop=True)
    return combined_dF

#running the function to combine the sequence files
all_acs = acs_5yr_csv_output_all(2019, "pa")

#this takes the output of the combining function and extracts the pieces that WPRDC would need. 
#Note - currently x in range(10), y in range (6,8) are for debugging purposes. 
#Note - the value column is in the wrong type - currently a str, should be int/float. waiting to build out the try/except statements (since some will be ".")
index_list = ["geoid", "value", "raw_value", "survey", "year", "table_id", "value_type"]
final_df = pd.DataFrame([])
dtw_dict = all_acs.to_dict('records')
x = 0
for x in range(10):
    for y in range(6, 8):
        staging_list = []
        staging_list.extend([logrecno_dict[dtw_dict[x][5]], dtw_dict[x][y], str(dtw_dict[x][y]), 'acs',
                             int(dtw_dict[x][1][:4]), seq_header_dF.loc[y - 6][0], dtw_dict[x][1][4:5].upper()])
        staging_list = pd.DataFrame(staging_list, index=index_list)
        final_df = pd.concat([final_df, staging_list.transpose()]).reset_index(drop=True)

print(final_df.transpose())
