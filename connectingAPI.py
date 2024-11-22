import requests
import json
import pandas as pd

def call_api():
    api_url = f'https://api.census.gov/data/2020/cps/asec/mar?get=A_HGA,PRDTRACE,HTOTVAL,PEHSPNON,A_SEX,A_MARITL&for=state:*'

    response = requests.get(api_url)
    print(response.text)

    csv = open("census_data_all.csv", "w")
    csv.write(response.text)
    csv.close()

def creating_mod_df(csv):
    df = pd.read_csv(csv)
    mod_df=df.copy()
    mod_df.drop('Unnamed: 7', axis=1, inplace=True)
    mod_df.rename(columns={'[["A_HGA"':'AttainedEd', 'PRDTRACE':'Race', 'HTOTVAL':'TotalIncome', 'PEHSPNON':'HispanicInd', 'A_SEX':'Sex', 'A_MARITL':'MaritalStatus','state]':'State'}, inplace=True)
    mod_df['AttainedEd'] = mod_df['AttainedEd'].str.replace("[\"", "")
    mod_df['AttainedEd'] = mod_df['AttainedEd'].str.replace("\"", "")
    mod_df['State'] = mod_df['State'].str.replace("]", "")
    return mod_df

def demo_descr(api_url):
    demo_api_url = f'{api_url}'
    response = requests.get(demo_api_url)
    json_response = response.json()
    table_values = json_response['values']['item']

    table_cols = ['DemoCode', "DemoValue"]

    demo_desc_map = pd.DataFrame(table_values, index= ([0]))
    demo_desc_map = demo_desc_map.transpose()
    demo_desc_map.reset_index(inplace=True)
    demo_desc_map.columns=table_cols
    return demo_desc_map

def mapping (mod_df, demo_marital_df, demo_attained_ed_df, demo_hispanic_ind_df, demo_race_df, demo_sex_df,state_df):
    
    mapping_df = {
    'HispanicInd': demo_hispanic_ind_df,
    'AttainedEd': demo_attained_ed_df,
    'Race': demo_race_df,
    'Sex': demo_sex_df,
    'MaritalStatus': demo_marital_df,
    'State': state_df
    }

    # Loop through each column and its mapping DataFrame
    for demo_col, demo_mapping_df in mapping_df.items():
        # Create a dictionary from the mapping DataFrame for faster lookup
        mapping_dict = dict(zip(demo_mapping_df['DemoCode'], demo_mapping_df['DemoValue']))
        mod_df[demo_col] = mod_df[demo_col].astype(str)
        demo_mapping_df['DemoCode'] = demo_mapping_df['DemoCode'].astype(str)
        mod_df[demo_col] = mod_df[demo_col].map(mapping_dict).fillna('Unknown')

    # Display the updated main_df
    return mod_df

#calling all the fxs
if __name__ == '__main__':

    #call_api()

    #undoing label encoding
    mod_df = creating_mod_df("census_data_all.csv")

    marital = 'https://api.census.gov/data/2020/cps/asec/mar/variables/A_MARITL.json'
    demo_marital_df = demo_descr(marital)

    attained_ed = 'https://api.census.gov/data/2020/cps/asec/mar/variables/A_HGA.json'
    demo_attained_ed_df = demo_descr(attained_ed)

    hispanic_ind = 'https://api.census.gov/data/2020/cps/asec/mar/variables/PEHSPNON.json'
    demo_hispanic_ind_df = demo_descr(hispanic_ind)

    sex = 'https://api.census.gov/data/2020/cps/asec/mar/variables/A_SEX.json'
    demo_sex_df = demo_descr(sex)

    race = 'https://api.census.gov/data/2020/cps/asec/mar/variables/PRDTRACE.json'
    demo_race_df = demo_descr(race)

    state = 'https://api.census.gov/data/2020/cps/asec/mar/variables/GESTFIPS.json'
    state_df = demo_descr(state)

    mod_all_states_df = mapping(mod_df, demo_marital_df, demo_attained_ed_df, demo_hispanic_ind_df, demo_race_df, demo_sex_df, state_df)

    mod_all_states_df.to_csv('mod_census_data_all.csv', sep=',',index=True, index_label='Idx')
