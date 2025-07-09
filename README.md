# NOTE

This repository has been archived. A updated paper and repository will be made available soon.

# effective_precip
Pixel-wise water balance to estimate effective precipitation

## Description of procedure to run model

1. `download_scripts/ee_data.py` gets OpenET ensemble ET, bias-corrected gridMET ETo,
   and (standard) gridMET precipitation data. All data resampled and clipped into
   grid matching `ucrb_aws.tif` (available water storage in root zone from gSSURGO).

2. `download_scripts/make_arrs.py` writes ET/ETo/precip data into 3D array for easier
   processing (could/should be combined with first step).

3. `do_interp.py` computes EToF as ET / ETo and linearly interpolates EToF pixel-wise
   to get daily EToF (and ET).

4. `run_ucrb.py` runs the model for the region. Most of the work is done by the
   function `do_wb` from `water_balance.py`, which takes available water storage
   (AWS) grid (2D), gridded precip time series (3D), and gridded interpolated ET
   time series (3D) as arguments.

5. `annual_epr.py` computes annual water year (Oct-Sept) effective precipitation and
   and writes the results out as geotiffs.

## Description of other code files

- `plot_epr.py` make figures of various model outputs like effective precipitation,
  ET of applied water, and fractions thereof.

- `compare_etdemands.py` compares field scale annual effective precipitation with
  values from ET demands.

- `compare_veget.py` compares field scale annual ET of precipitation with values
  values from from VegET.

