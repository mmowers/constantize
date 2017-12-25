from __future__ import division
import gdxpds
import pandas as pd
import os

base_year = 2024

distPVSwitch = 'StScen2017_Mid_Case'
distPVFiles = ['distPVcap', 'distPVelecprice']

WindCaseSwitch = 'ATB_2017_Wind_Mid_Cost'

#Files to use
gdx_input_files = [
    {'filename': 'input', 'path': '../inout/includes/'},
    {'filename': 'PrescriptiveBuilds', 'path': '../inout/includes/'},
    {'filename': 'PrescriptiveRetirements', 'path': '../inout/includes/'},
    {'filename': 'rggi', 'path': '../inout/includes/'},
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
    'NG_Forecast',
    # 'PrescriptiveBuildshydcats',
    # 'PTCqallyears',
    # 'Ref_NG_Foresight',
    # 'UPV_Deg_Foresight_all'
    # 'UPV_FinwITC_all',
    # 'UPV_FinwPTC_all',
    # 'W_FinwPTC_allyears',
    # 'WindBuildsIn',
    # 'WindRetireIn',
    # 'WPTC',
]

#parameters to zero out post-base-year
zeroed_params = [
    #within PrescriptiveBuilds.gdx
    'PrescriptiveBuildsNonQn',
    'PrescriptiveBuildsnqct',
    'PrescriptiveBuildsStorage',
    'PrescriptiveBuildsWind',
    'WindBuilds',

    #within PrescriptiveRetirements.gdx
    'ABBPrescriptiveRetNuke60',
    'ABBPrescriptiveRetNuke80',
    'ABBPrescriptiveRetNukeEarly',
    'ABBPrescriptiveRetNukeRef',
    'PrescriptiveRet',

    #within input.gdx
    'PrescriptiveBuildshydcats',
    'WindBuildsIn',
    'WindRetireIn',
    'UPV_Deg_Rate_all',
    'DUPV_Deg_Rate_all',
    'DistPV_Deg_Rate_all',
]

#clear outputs
for dirname in ['out','out/changed']:
    for f in os.listdir(dirname):
        if f.endswith(".gdx") or f.endswith(".csv"):
            os.remove(os.path.join(dirname, f))

#First do csv modifications
#distributed PV:
for f in distPVFiles:
    df = pd.read_csv('../inout/includes/dSolar_Inputs/' + f + '_' + distPVSwitch + '.csv', index_col=0)
    for y in df.columns:
        if int(y) > base_year:
            df[y] = df[str(base_year)]
    df.to_csv('out/' + f + '_' + distPVSwitch + '.csv')

#Wind:
dfs = []
dfw = pd.read_csv('../inout/includes/Wind_Inputs/' + WindCaseSwitch + '.csv', index_col=0)
index_cols = ['Tech', 'Wind class']
val_cols = ['CFc', 'Cap cost 1000$/MW', 'Fixed O&M 1000$/MW-yr', 'Var O&M $/MWh']
for v in val_cols:
    df = dfw.pivot_table(index=index_cols, columns='Year', values=v).reset_index()
    df['type'] = v
    yr_cols = [j for j in df.columns if j not in index_cols + ['type']]
    for y in yr_cols:
        if y > base_year:
            df[y] = df[base_year]
    dfs.append(df)
dfo = pd.concat(dfs).reset_index(drop=True)
dfo = dfo.melt(id_vars=index_cols + ['type'], var_name='Year', value_name= 'Value')
dfo = dfo.pivot_table(index=index_cols + ['Year'], columns='type', values='Value').reset_index()
dfo.to_csv('out/' + WindCaseSwitch + '.csv', index=False)

#Now do general gdx modifications for modifying values after base year and modifying lifetime parameters
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
                        col_names = [str(j) for j in range(len(df.columns))]
                        df.columns = col_names
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
                        df = df.melt(id_vars=index_cols, var_name=yr, value_name= val)
                        # df = pd.melt(df, id_vars=index_cols, var_name='yr', value_name= 'Value')
                        #remove na
                        df = df[pd.notnull(df[val])]
                        #convert years back to strings
                        df[yr] = df[yr].astype(str)
                        #reorder columns to original order
                        df = df[col_names]
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
        gdx.write('out/' + gdxfile['filename'] + '.gdx')
    with gdxpds.gdx.GdxFile() as gdx:
        for symbol in changed_list:
            gdx.append(symbol)
        gdx.write('out/changed/' + gdxfile['filename'] + '.gdx')
