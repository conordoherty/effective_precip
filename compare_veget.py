from rasterstats import zonal_stats
import pandas as pd
import matplotlib.pyplot as plt
from statsmodels.api import OLS

shp_fn = 'field_shapefiles/fields_fully_within.shp'
veg_shp_fn = 'field_shapefiles/fields_fully_within_4326.shp'
#in_dir = 'epr_rasters_avg_aws'
in_dir = 'epr_rasters'
veget_dir = 'veget_comp'
#suff = '_avg_aws'
suff = ''

df = pd.DataFrame()
for yr in range(2019, 2021):
    zs = zonal_stats(shp_fn, f'{veget_dir}/et_of_precip_{yr}.tif',
                     stats='mean', geojson_out=True)

    yr_df = pd.DataFrame([zs[i].properties for i in range(len(zs))])
    yr_df = yr_df[['OBJECTID_1', 'mean']]
    yr_df = yr_df.rename(columns={'mean': 'et_of_pr'})
    yr_df = yr_df.rename(columns={'OBJECTID_1': 'id'})
    yr_df['year'] = yr

    zs = zonal_stats(shp_fn, f'{in_dir}/pr_{yr}.tif',
                     stats='mean', geojson_out=True)

    yr_df2 = pd.DataFrame([zs[i].properties for i in range(len(zs))])
    yr_df2 = yr_df2[['OBJECTID_1', 'mean']]
    yr_df2 = yr_df2.rename(columns={'mean': 'pr'})
    yr_df2 = yr_df2.rename(columns={'OBJECTID_1': 'id'})
    yr_df2['year'] = yr

    yr_df = yr_df.merge(yr_df2, on=['id', 'year'])

    veg_zs = zonal_stats(veg_shp_fn, f'{veget_dir}/wy_{yr}_etasw_up.tif',
                         stats='mean', geojson_out=True)
    veg_df = pd.DataFrame([veg_zs[i].properties for i in range(len(veg_zs))])
    veg_df = veg_df[['OBJECTID_1', 'mean']]
    veg_df = veg_df.rename(columns={'mean': 'etasw'})
    veg_df = veg_df.rename(columns={'OBJECTID_1': 'id'})
    veg_df['year'] = yr

    yr_df = yr_df.merge(veg_df, on=['id', 'year'])


    df = pd.concat((df, yr_df))

    yr_df = yr_df.dropna()
    low = yr_df[['et_of_pr', 'etasw']].min().min()
    high = yr_df[['et_of_pr', 'etasw']].max().max()
    
    yr_df['const'] = 1
    mod = OLS(yr_df.etasw, yr_df[['et_of_pr', 'const']])
    fit = mod.fit()
    
    fig = plt.figure()
    pad = 6
    plt.plot([low-pad, high+pad], [low-pad, high+pad], label='1:1 line', color='gray')
    plt.plot(yr_df.et_of_pr, fit.fittedvalues, 'k:', label='ETaSW ~ ET of Pr + const')
    plt.plot(yr_df.et_of_pr, yr_df.etasw, '.', markersize=4, label='Fields')
    plt.gca().set_aspect('equal')
    plt.legend(fontsize=8)
    plt.ylabel('VegET ETaSW (mm)')
    plt.xlabel('ET of precip (mm)')

    plt.text(high-pad/2, low+pad, f'slope: {fit.params.et_of_pr:.3f}', ha='right')
    plt.text(high-pad/2, low, f'intercept: {fit.params.const:.1f}', ha='right')
    plt.text(high-pad/2, low-pad, f'R^2: {fit.rsquared:.2f}', ha='right')
    plt.title(f'Water year {yr}')

    plt.tight_layout()
    plt.savefig(f'{veget_dir}/comp_{yr}.png')
    #plt.show()



'''
for yr in range(19, 22):
    wyr_cols = [f'{i}_{yr-1}' for i in range(10, 13)]\
               + [f'0{i}_{yr}' for i in range(1, 10)]
    et_dem_prz_yr_cols = et_dem[[f'P_rz_{x}' for x in wyr_cols]]
    et_dem_prz_yr = pd.DataFrame({'id': et_dem.DRI_ID,
                                  'epr_etd': et_dem_prz_yr_cols.sum(axis=1)})

    et_dem_ppt_yr_cols = et_dem[[f'PPT_{x}' for x in wyr_cols]]
    et_dem_ppt_yr = pd.DataFrame({'id': et_dem.DRI_ID,
                                  'pr_etd': et_dem_ppt_yr_cols.sum(axis=1)})

    et_dem_gm_yr = pd.DataFrame({'id': et_dem.DRI_ID,
                                 'gm_id': et_dem.GRIDMET_ID})

    et_dem_yr = et_dem_ppt_yr.merge(et_dem_prz_yr, on=['id'])\
                             .merge(et_dem_gm_yr, on=['id'])
    et_dem_yr['year'] = 2000+yr
    et_dem_yr['id'] = et_dem_yr.id.apply(lambda x: x[3:]).astype('int')

    df = df_epr.merge(et_dem_yr, on=['id', 'year'])
    print(df.shape)
    low = df[['epr', 'epr_etd']].min().min()
    high = df[['epr', 'epr_etd']].max().max()
    
    df['const'] = 1
    df = df.dropna()
    full_df = df.copy()
    mod = OLS(df.epr_etd, df[['epr', 'const']])
    fit = mod.fit()
    
    plt.figure()

    pad = 11
    plt.plot([low-pad, high+pad], [low-pad, high+pad], label='1:1 line', color='gray')
    plt.plot(full_df.epr, fit.fittedvalues, 'k:', label='')
    for gm_id in full_df.gm_id.unique():
        df = full_df[full_df.gm_id==gm_id]
        plt.plot(df.epr, df.epr_etd, '.', label=gm_id, markersize=4)

    plt.text(high-pad/2, low+pad, f'slope: {fit.params.epr:.3f}', ha='right')
    plt.text(high-pad/2, low+pad/2, f'intercept: {fit.params.const:.1f}', ha='right')
    plt.text(high-pad/2, low, f'R^2: {fit.rsquared:.2f}', ha='right')
    
    plt.ylim(low-pad, high+pad)
    plt.xlim(low-pad, high+pad)
    plt.legend(fontsize=8, loc='upper left')
    plt.ylabel('ET demands P_rz (mm)')
    plt.xlabel('Eff precip (mm)')
    plt.title(f'Water year 20{yr}'+f' {suff}')
    plt.gca().set_aspect('equal')
    plt.tight_layout()
    plt.savefig(f'comparison_plots/vs_et_demands_{yr}{suff}.png')
    plt.close()

    full_df['epr_frac'] = full_df.epr / full_df.pr
    full_df['epr_frac_etd'] = full_df.epr_etd / full_df.pr

    low = full_df[['epr_frac', 'epr_frac_etd']].min().min()

    plt.figure()
    for gm_id in full_df.gm_id.unique():
        df = full_df[full_df.gm_id==gm_id]
        plt.plot(df.epr_frac, df.epr_frac_etd, '.', label=gm_id, markersize=4)

    plt.ylim(low-.05, 1.05)
    plt.xlim(low-.05, 1.05)
    plt.ylabel('ET demands P_rz / Precip')
    plt.xlabel('Eff precip / Precip')
    plt.title(f'Water year 20{yr}'+f' {suff}')
    plt.gca().set_aspect('equal')
    plt.legend(fontsize=8, loc='upper left')
    plt.tight_layout()
    plt.savefig(f'comparison_plots/vs_et_demands_frac_{yr}{suff}.png')
    plt.close()
    #plt.show()

'''
