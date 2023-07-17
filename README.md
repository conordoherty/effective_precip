# effective_precip
Pixel-wise water balance to estimate effective precipitation

## description of code

1. `download_scripts/ee_data.py` gets OpenET ensemble ET, bias-corrected gridMET ETo,
   and (standard) gridMET precipitation data. All data resampled and clipped into
   grid matching `ucrb_aws.tif` (available water storage in root zone from gSSURGO).

2. `download_scripts/make_arrs.py` writes ET/ETo/precip into 3D array for easier
   processing (could/should be combined with first step)

Data in SIMS drive folder: https://drive.google.com/drive/folders/18w9kVVmhW1EsT6mGWhCHmRJeUUsq6Pxh?usp=share_link

Data in OpenET drive folder: https://drive.google.com/drive/folders/1ZIks0mq7qJLSYPaRxjPBEPW-SRrJcMRO?usp=share_link
