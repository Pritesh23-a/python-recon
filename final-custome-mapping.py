import numpy as np
import pandas as pd

# Load Excel files
bbg_df = pd.read_excel("bbg.xlsx")
murex_df = pd.read_excel("murex.xlsx")
mapping_df = pd.read_excel("MAPPING.xlsx")


# Dictionary for deal mapping: BBG ID → Murex ID
deal_id = dict(zip(mapping_df['BBG_deal_ID'], mapping_df['Murex_Deal_ID']))
# Dictionary for counterparty mapping: murex Name → BBG Name
counterparty_mapping_dictionary = dict(
    zip(mapping_df['COUNTERPARTY_Murex'].str.strip(), mapping_df['COUNTERPARTY_BBG'].str.strip()))

print(counterparty_mapping_dictionary)
# Sorting BBG DataFrame based on mapping BBG ID key
bbg_df_sorted = bbg_df.sort_values(
    by=['Deal ID'], key=lambda x: pd.Categorical(x, categories=deal_id.keys(), ordered=True)
)

# Sorting Murex DataFrame based on mapping Murex ID
murex_df_sorted = murex_df.sort_values(
    by=['Deal ID'], key=lambda x: pd.Categorical(x, categories=list(deal_id.values()), ordered=True)
)

# Adding new column in BBG DataFrame for mapped Murex Deal ID
bbg_df_sorted['murex_Deal_id'] = bbg_df_sorted['Deal ID'].map(deal_id).fillna(np.nan)

print("bbg df sorted--------")
print(bbg_df_sorted)

#TODO
# Drop rows where 'murex_Deal_id' is NaN to avoid merge issues
#bbg_df_sorted = bbg_df_sorted.dropna(subset=['murex_Deal_id'])

# Merging both DataFrames on mapped deal IDs
merge_df = bbg_df_sorted.merge(
    murex_df_sorted, left_on='murex_Deal_id', right_on='Deal ID', indicator=True, suffixes=('_bbg', '_murex')
)

total_col = ['COUNTERPARTY','WE', 'TRANSACTION TYPE', 'PRODUCT','Trade Date','Fixed Rate (%)']

unmatched_bbg_murex = bbg_df_sorted[~bbg_df_sorted['murex_Deal_id'].isin(merge_df['murex_Deal_id'])]

print("unmatched murex df+++++++++++")
print(unmatched_bbg_murex)
unmapped_columns_to_compare = ['WE', 'TRANSACTION TYPE', 'PRODUCT','Trade Date','Fixed Rate (%)']

for col in unmapped_columns_to_compare:
    bbg_col, murex_col = f'{col}_bbg', f'{col}_murex'

    # Ensure both columns exist in the DataFrame before transformation
    if bbg_col in merge_df.columns and murex_col in merge_df.columns:
        merge_df[murex_col] = merge_df[murex_col].astype(str).str.strip()
        merge_df[bbg_col] = merge_df[bbg_col].astype(str).str.strip()
        merge_df[f'NEW_{col}'] = merge_df.apply(
            lambda row: (
                f"{row[murex_col]} / {row[bbg_col]}"  # If values are different
                if pd.notna(row[bbg_col]) and pd.notna(row[murex_col]) and row[bbg_col] != row[murex_col]
                else  f"{row[murex_col]} - {row[bbg_col]}"   # If values are same

                if row[bbg_col] and row[murex_col] !='nan'
                else '#NA'  # Assign "#NA" if either value is missing
                # if pd.notna(row[bbg_col]) and pd.notna(row[murex_col])
                # else '#NA'  # Assign "#NA" if either value is missing
            ),
            axis=1
        )




print("----Transformed unmapped columns DataFrame-----------")
print(merge_df)

mapped_col_to_compare = ["COUNTERPARTY"]

for col in mapped_col_to_compare:
    bbg_col, murex_col = f'{col}_bbg', f'{col}_murex'

    # Ensure both columns exist in the DataFrame before transformation
    if bbg_col in merge_df.columns and murex_col in merge_df.columns:
        merge_df[murex_col] = merge_df[murex_col].astype(str).str.strip()
        merge_df[bbg_col] = merge_df[bbg_col].astype(str).str.strip()
        merge_df[f'NEW_{col}'] = merge_df.apply(
            lambda row: (
                f"{row[murex_col]} / {row[bbg_col]}"  # If values are different
                if pd.notna(row[bbg_col]) and pd.notna(row[murex_col]) and row[bbg_col] != counterparty_mapping_dictionary.get(row[murex_col])
                else  f"{row[murex_col]} - {row[bbg_col]}"   # If values are same
                if row[bbg_col] and row[murex_col] !='nan'
                else '#NA'  # Assign "#NA" if either value is missing
                # if pd.notna(row[bbg_col]) and pd.notna(row[murex_col])
                # else '#NA'  # Assign "#NA" if either value is missing
            ),
            axis=1
        )































merge_df.to_excel("output-merge.xlsx", index=False)