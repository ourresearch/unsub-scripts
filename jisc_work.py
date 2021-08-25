# coding: utf-8

# heroku local:run ipython
import pandas as pd
from unsub_database import make_cursor

# make cursor
cursor = make_cursor()

# ISSN-L's for all JISC institutions
cursor.execute("select distinct issn_l from jump_counter where package_id ilike 'package-jiscels%'")
allinst_issns: pd.DataFrame = cursor.fetch_dataframe()

# get all JISC institution package IDs
cursor.execute("select distinct package_id from jump_account_package where package_id ilike 'package-jiscels%'")
jisc_inst: pd.DataFrame = cursor.fetch_dataframe()

# loop through each JISC institution package ID to get issn's in 
#   individual institutions NOT the same as the all institutions list
out = []
for x in jisc_inst['package_id'].to_list():
	print(x)
	cursor.execute("select distinct issn_l from jump_counter where package_id ilike '{}'".format(x))
	issns_sing: pd.DataFrame = cursor.fetch_dataframe()
	diffed = list(set(allinst_issns['issn_l'].to_list()) - set(issns_sing['issn_l'].to_list()))
	new_df = pd.DataFrame({'issn_l': diffed})
	new_df = new_df.assign(package_id = x)
	out.append(new_df)
len(out)
out[0]
out[100]
lengths = [len(z) for z in out]
sum(lengths)

# combine them
alldata = pd.concat(out)
alldata = alldata.assign(journal_name = None, total = 0, report_year = 2020, 
	report_name = "trj2", report_version = 5, metric_type = "No_License", 
	yop = None, access_type = None, created = "2021-08-17")

# write to csv
alldata.to_csv("jisc.csv", index=False)
