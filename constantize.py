from __future__ import division
import gdxpds
import pandas as pd

base_year = 2020

#Files to use
gdx_input_files = [
{'filename': 'input', 'path': '../inout/includes/'},
]


#excluded params:
excluded_params = [
    # 'BioEnergyCropFrac_allyears',
    # 'CSP_Fin',
    # 'HydPTC_allyears',
    # 'Hydro_FinwPTC_all',
    # 'HydroRECUB',
    # 'HydUpgAdderNew',
    # 'HydUpgMaxNew',
    # 'load_proj_mult',
    # 'load_proj_mult_canada',
    # 'load_proj_mult_mexico',
    # 'NG_Forecast',
    # 'PrescriptiveBuildshydcats',
    #'PTCqallyears',
    # 'Ref_NG_Foresight',
    # 'UPV_FinwITC_all',
    # 'UPV_FinwPTC_all',
    # 'W_FinwPTC_allyears',
    # 'WindBuildsIn',
    # 'WindRetireIn',
    # 'WPTC',
]

#parameters to zero out post-base-year
zeroed_params = [
    'PrescriptiveBuildshydcats',
    'WindBuildsIn',
    'WindRetireIn',
]

for gdxfile in gdx_input_files:
    symbol_list = []
    changed_list = []
    with gdxpds.gdx.GdxFile(lazy_load=False) as f:
        f.read(gdxfile['path'] + gdxfile['filename'] + '.gdx')
        #for each symbol, find columns that match 4-digit years and modify so that all values for
        #later years are equal to the value for the specified year above. 
        for symbol in f:
            df = symbol.dataframe.copy()
            if symbol.name not in excluded_params and not df.empty:
                for i in range(len(df.columns)):
                    col = df.iloc[:, i]
                    #check if this column is one of the set columns and full of all years
                    if col.name == '*' and col.str.match('^20[0-9]{2}$').all() and str(base_year) in col.values:
                        #rename columns to their indices
                        df.columns = [str(j) for j in range(len(df.columns))]
                        #turn years into int so that we can find later years
                        yr = str(i)
                        df[yr] = pd.to_numeric(df[yr])
                        val = str(len(df.columns) - 1) 
                        #gather index columns
                        index_cols = [j for j in df.columns if j not in [yr, val]]
                        #turn into pivot table
                        df = df.pivot_table(index=index_cols, columns=yr, values=val)
                        if index_cols != []:
                            df = df.reset_index()
                        #iterate over year columns and set values for later years equal to specified base_year
                        yr_cols = [j for j in df.columns if j not in index_cols]
                        if symbol.name in zeroed_params:
                            for y in yr_cols:
                                if y > base_year:
                                    df = df.drop(y, 1)
                        else:
                            for y in yr_cols:
                                if y > base_year:
                                    df[y] = df[base_year]
                        #melt back into flat dataframe
                        df = df.melt(id_vars=index_cols, var_name='yr', value_name= 'Value')
                        # df = pd.melt(df, id_vars=index_cols, var_name='yr', value_name= 'Value')
                        #remove na
                        df = df[pd.notnull(df['Value'])]
                        #convert years back to strings
                        df['yr'] = df['yr'].astype(str)
                        #rename columns back to *
                        df.columns = ['*']*(len(df.columns) - 1) + ['Value']
                        symbol.dataframe = df
                        changed_list.append(symbol)
                        #only one columns is used to change values, so we break:
                        break
            symbol_list.append(symbol)

    with gdxpds.gdx.GdxFile() as gdx:
        for symbol in symbol_list:
            gdx.append(symbol)
        gdx.write(gdxfile['filename'] + '.gdx')
    with gdxpds.gdx.GdxFile() as gdx:
        for symbol in changed_list:
            gdx.append(symbol)
        gdx.write(gdxfile['filename'] + '_changed.gdx')
