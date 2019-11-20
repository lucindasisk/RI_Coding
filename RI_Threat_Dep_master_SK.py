#!/usr/bin/env python
# coding: utf-8

# # Clean and Code UCLA RI Data - Adult Version
# ### Usage:

# In[1]:


# Import packages
import pandas as pd
import os
import numpy as np
from datetime import date
from glob import glob
today = str(date.today())
pd.options.display.max_rows = 999
pd.options.display.max_columns = 999


# In[8]:


# #Set path variables and read in data set
# user = os.path.expanduser('~')
# home = os.path.join(user, 'Box Sync/PhD/Projects/Shapes/Shapes_RI')
# out = os.path.join(home, 'Outputs')
# V1 = os.path.join(home,'Data/V1_UCLARIADULT52218_DATA_2019-08-20_1332.csv')
# V2 = os.path.join(home,'Data/V2_UCLARIADULT52218_DATA_2019-08-20_1332.csv')
# V3 = os.path.join(home,'Data/V3_UCLARIADULT52218_DATA_2019-08-20_1333.csv')
# V4 = os.path.join(home,'Data/V4_UCLARIADULT52218_DATA_2019-08-20_1334.csv')
# V5 = os.path.join(home,'Data/V5_UCLARIADULT52218_DATA_2019-08-20_1334.csv')
# V6 = os.path.join(home,'Data/V6_UCLARIADULT52218_DATA_2019-08-20_1335.csv')
# V7 = os.path.join(home,'Data/V7_UCLARIADULT52218_DATA_2019-08-20_1335.csv')
# V8 = os.path.join(home,'Data/V8_UCLARIADULT52218_DATA_2019-08-20_1336.csv')
# V9 = os.path.join(home,'Data/V9_UCLARIADULT52218_DATA_2019-08-20_1354.csv')


# In[2]:


# Set path variables and read in data set (SK 11/19/19)
user = os.path.expanduser('~')
home = os.path.join(user, 'Box Sync/PhD/Grants/NRSA/Pilot_Analyses')
out = os.path.join(home, 'RI_Outputs')
V1 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V1.csv')
V2 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V2.csv')
V3 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V3.csv')
V4 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V4.csv')
V5 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V5.csv')
V6 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V6.csv')
V7 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V7.csv')
V8 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V8.csv')
V9 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V9.csv')


# In[3]:


# Set path variables and read in data set (SK 11/19/19)
user = os.path.expanduser('~')
home = os.path.join(user, 'Box Sync/PhD/Grants/NRSA/Pilot_Analyses')
out = os.path.join(home, 'RI_Outputs')
V1 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V1.csv')
V2 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V2.csv')
V3 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V3.csv')
V4 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V4.csv')
V5 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V5.csv')
V6 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V6.csv')
V7 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V7.csv')
V8 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V8.csv')
V9 = os.path.join(home, 'RI_Downloads/UCLARIADULT52218_DATA_V9.csv')


# ## Step 1: Clean Raw Data

# In[4]:


def get_cols(df):
    empty_cols = []
    for col_num in range(4, len(df.columns)):
        try:
            col = df.iloc[:, col_num].dropna().astype(int)
            if col.sum() < 1:
                empty_cols.append(col_num)
            else:
                pass
        except:
            print('couldnt index column {}'.format(col_num))
    return empty_cols


# In[5]:


# Function to drop empty columns and replace 999 with NaN (get_cols() function nested within)
def clean_cols(path, version):
    print('working on {}'.format(version))
    dset = pd.read_csv(path, header=0)
    dset = dset.replace('999', np.nan)
    dset = dset.replace(999, np.nan)
    dset_clean = dset.dropna(axis=1, how='all')
    empty_cols = get_cols(dset_clean)
    dset_clean_empty = dset_clean.drop(
        dset_clean.columns[empty_cols], axis=1).dropna(axis=1, how='all')
    return dset_clean_empty


# In[6]:


# Run function to clean all versions
v1_clean = clean_cols(V1, 'v1')
v2_clean = clean_cols(V2, 'v2')
v3_clean = clean_cols(V3, 'v3')
v4_clean = clean_cols(V4, 'v4')
v5_clean = clean_cols(V5, 'v5')
v6_clean = clean_cols(V6, 'v6')
v7_clean = clean_cols(V7, 'v7')
v8_clean = clean_cols(V8, 'v8')
v9_clean = clean_cols(V9, 'v9')


# In[7]:


# Merge cleaned versions together
m1 = pd.merge(v1_clean, v2_clean, on='ucla_a_id', how='outer')
m2 = pd.merge(m1, v3_clean, on='ucla_a_id', how='outer')
m3 = pd.merge(m2, v4_clean, on='ucla_a_id', how='outer')
m4 = pd.merge(m3, v5_clean, on='ucla_a_id', how='outer')
m5 = pd.merge(m4, v6_clean, on='ucla_a_id', how='outer')
m6 = pd.merge(m5, v7_clean, on='ucla_a_id', how='outer')
m7 = pd.merge(m6, v8_clean, on='ucla_a_id', how='outer')
m8 = pd.merge(m7, v9_clean, on='ucla_a_id', how='outer')

final = m8
final.to_csv(os.path.join(
    out, 'All_UCLA_RI_versions_Merged_Cleaned_{}.csv'.format(today)), index=False)


# ## Step 2: Code Data

# In[8]:


# Boilerplate code to parallelize operations
def parallelize_dataframe(df, func, n_cores=4):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df


# In[24]:


# Create threat and dep variables
thr_cols = [col for col in final.columns if 'icthr' in col]
dep_cols = [col for col in final.columns if 'icdep' in col]
thr_cols.append('ucla_a_id')
dep_cols.append('ucla_a_id')


# In[25]:


# Select columns from data frame; create final_summed variables
df_thr = final[thr_cols]
df_dep = final[dep_cols]
df_thr['num_thr_ev'] = df_thr.sum(axis=1)
df_dep['num_dep_ev'] = df_dep.sum(axis=1)
final_summed_thr = df_thr[['ucla_a_id', 'num_thr_ev']]
final_summed_dep = df_dep[['ucla_a_id', 'num_dep_ev']]
final_summed = pd.merge(final_summed_thr, final_summed_dep,
                        on='ucla_a_id', how='outer')


# In[10]:


# Set up loops to create new dfs by age
events = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k',
          'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'x', 'y']
numoccs = range(1, 32)  # number of occurrences -- up to 31 for each event


# In[11]:


# Empty lists to append threat/dep scores
thr_df = []
dep_df = []
all_df = []

# Define function to count number of endorsements of threat or deprivation
# Where e is events and x is numoccs


def score_num(dset, e, x, thde):
    data = dset.iloc[i]
    print('Working on row {}'.format(i))
    try:
        if thde == 'icthr':
            if data['ucla_a_{}_e{}_{}'.format(e, x, thde)] == int('1'):
                subthr = str(data['ucla_a_id'])
                itemthr = str(data['ucla_a_{}_e{}_{}'.format(e, x, thde)])
                asevthr = str(data['ucla_a_{}_e{}_avgsev'.format(e, x)])
                wsevthr = str(data['ucla_a_{}_e{}_worsev'.format(e, x)])
                agethr = str(data['ucla_a_{}_e{}_age'.format(e, x)])
                durthr = str(data['ucla_a_{}_e{}_dur'.format(e, x)])
                daythr = str(data['ucla_a_{}_e{}_day'.format(e, x)])
                print('appending data for {}'.format(subthr))
                thr_df.append([subthr, agethr, itemthr,
                               asevthr, wsevthr, durthr, daythr])
            else:
                pass
        elif thde == 'icdep':
            if data['ucla_a_{}_e{}_{}'.format(e, x, thde)] == int('1'):
                subdep = str(data['ucla_a_id'])
                itemdep = str(data['ucla_a_{}_e{}_{}'.format(e, x, thde)])
                asevdep = str(data['ucla_a_{}_e{}_avgsev'.format(e, x)])
                wsevdep = str(data['ucla_a_{}_e{}_worsev'.format(e, x)])
                agedep = str(data['ucla_a_{}_e{}_age'.format(e, x)])
                durdep = str(data['ucla_a_{}_e{}_dur'.format(e, x)])
                daydep = str(data['ucla_a_{}_e{}_day'.format(e, x)])
                print('appending data for {}'.format(subdep))
                dep_df.append([subdep, agedep, itemdep,
                               asevdep, wsevdep, durdep, daythr])
            else:
                pass
        elif thde == 'all':
            if data['ucla_a_{}_e{}_cod'.format(e, x, thde)] == int('1'):
                suball = str(data['ucla_a_id'])
                itemall = str(data['ucla_a_{}_e{}_cod'.format(e, x, thde)])
                asevall = str(data['ucla_a_{}_e{}_avgsev'.format(e, x)])
                wsevall = str(data['ucla_a_{}_e{}_worsev'.format(e, x)])
                ageall = str(data['ucla_a_{}_e{}_age'.format(e, x)])
                durall = str(data['ucla_a_{}_e{}_dur'.format(e, x)])
                dayall = str(data['ucla_a_{}_e{}_day'.format(e, x)])
                print('appending data for {}'.format(suball))
                all_df.append([suball, ageall, itemall,
                               asevall, wsevall, durall, dayall])
            else:
                pass
        else:
            pass
    except:
        pass


# In[12]:


# Perform actual counts
for i in range(0, len(final)):
    for e in events:
        for x in numoccs:
            score_num(final, e, x, 'icthr')
            score_num(final, e, x, 'icdep')
            score_num(final, e, x, 'all')


# In[33]:


# Put counts into dataframes
thr_data = pd.DataFrame(thr_df).rename(columns={
    0: 'ucla_a_id', 1: 'age_at_occ', 2: 'endorse_thr', 3: 'avg_sev', 4: 'worst_sev', 5: 'chr_dur', 6: 'chr_day'})
#dep_data = pd.DataFrame(dep_df).rename(columns={0:'ucla_a_id',1:'age_at_occ',2:'endorse_dep',3:'avg_sev',4:'worst_sev',5:'chr_dur',6:'chr_day'})
all_data = pd.DataFrame(all_df).rename(columns={
    0: 'ucla_a_id', 1: 'age_at_occ', 2: 'endorse_any', 3: 'avg_sev', 4: 'worst_sev', 5: 'chr_dur', 6: 'chr_day'})


# In[34]:


# write data to CSV for checking
thr_data.to_csv(os.path.join(
    out, 'Cleaned_threat_endorsements_{}.csv'.format(today)), index=False)
#dep_data.to_csv(os.path.join(out,'Cleaned_dep_endorsements_{}.csv'.format(today)), index=False)
all_data.to_csv(os.path.join(
    out, 'Cleaned_all_endorsements_{}.csv'.format(today)), index=False)


# In[35]:


# Reset dataframes for summing and aggregation
thr_data1 = thr_data.set_index('ucla_a_id').astype(float)
# dep_data1=dep_data.set_index('ucla_a_id').astype(float)
all_data1 = all_data.set_index('ucla_a_id').astype(float)


# In[38]:


# Transform threat endorsements
thr_wide = thr_data1.pivot_table(index='ucla_a_id',
                                 columns='age_at_occ',
                                 values=['endorse_thr',
                                         'avg_sev',
                                         'worst_sev',
                                         'chr_dur',
                                         'chr_day'],
                                 aggfunc=sum)

# Transform all endorsements
# dep_wide = dep_data1.pivot_table(index='ucla_a_id',
#                                 columns='age_at_occ',
#                                 values=['endorse_dep',
#                                         'avg_sev',
#                                         'worst_sev',
#                                         'chr_dur',
#                                         'chr_day'],
#                                aggfunc=sum)

# Transform all endorsements
all_wide = all_data1.pivot_table(index='ucla_a_id',
                                 columns='age_at_occ',
                                 values=['endorse_any',
                                         'avg_sev',
                                         'worst_sev',
                                         'chr_dur',
                                         'chr_day'],
                                 aggfunc=sum)
all_wide['summed_vars'] = endorse_any_*


# In[39]:


# Wide dataframes to CSV for analysis
thr_wide.to_csv(os.path.join(
    out, 'Cleaned_WIDE_threat_endorsements_{}.csv'.format(today)))
#dep_wide.to_csv(os.path.join(out, 'Cleaned_WIDE_dep_endorsements_{}.csv'.format(today)))
all_wide.to_csv(os.path.join(
    out, 'Cleaned_WIDE_any_endorsements_{}.csv'.format(today)))


#
#   # Code Graveyard

# In[ ]:


d = []
data = dset_cleaned.iloc[3]
x = str(data['ucla_a_id'])
d.append(x)
d


# In[ ]:


thr_data = pd.DataFrame(thr_df)
thr_data


# In[ ]:


threat_df1 = pd.DataFrame(thr_df).groupby(0).sum().reset_index()
threat_df = threat_df1.rename(columns={0: 'ucla_a_id', 1: "num_threat"})

depriv_df1 = pd.DataFrame(dep_df).groupby(0).sum().reset_index()
depriv_df = depriv_df1.rename(columns={0: 'ucla_a_id', 1: "num_dep"})

merged_df = pd.merge(threat_df, depriv_df, on='ucla_a_id', how='outer')

finaldf = pd.merge(merged_df, dset, on='ucla_a_id', how='outer')


# In[ ]:


merged_df.to_csv(os.path.join(home, 'LS_scored_traumaData_' + today + '.csv'))


# In[ ]:


# In[ ]:


# In[ ]:


# In[ ]:


# Create thr/dep by age function
pairs = []


def create_age_exp_pairs(df, typ_exp):
    criteria1 = df.columns.str.contains('age')
    criteria2 = df.columns.str.contains('ic{}'.format(typ_exp))
    criteria_all = criteria1 | criteria2
    cols = df.columns[criteria_all]
    new_df = df[cols]
    return new_df


# Create thr/dep by age data frames
thr_age_df = create_age_exp_pairs(
    dset_cleaned, 'thr').dropna(axis=1, how='all')
dep_age_df = create_age_exp_pairs(
    dset_cleaned, 'dep').dropna(axis=1, how='all')
