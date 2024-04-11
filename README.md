# DASH Launch Hackathon
### 19 April 2024

## How to run
#### (Optional) Set up GDAL on a cluster
1. If not already installed, run the `setup_gdal` notebook to generate an init script in the specified location in the **Workspace** (or UC Volumes).
2. On the cluster configuration page, under Advanced Options and the Init Scripts tab, add the path to your newly created init script.

#### UK Flood
This is the simpler example of loading geoJSON datasets downloaded from the [DEFRA Data Services Platform](https://environment.data.gov.uk/). The example uses:
* [Flood Warning Areas](https://environment.data.gov.uk/dataset/87e5d78f-d465-11e4-9343-f0def148f590)
* [Historic Flood Map](https://environment.data.gov.uk/dataset/889885c0-d465-11e4-9507-f0def148f590)
* [Agricultural Land Classification](https://environment.data.gov.uk/dataset/af1b847b-037b-4772-9c31-7edf584522aa)

for all available areas.

1. Download and upload the files to the DBFS `FileStore` or a mounted ADLS/Blob storage.
2. Change the path(s) for each file in the notebook.
3. Run the notebook.

#### NY Flood

This is the more complicated example, which aims to build a dataset that can help predict flood risk by area/tile.

This *should* run in a self-contained fashion as long as the paths and catalog/schema names are updated.
1. Running `00 Download Data` (with updated paths) should download all the raw files to the DBFS.
2. `01 Load Data To Delta` goes through each directory and loads the files of various formats into Delta format.
3. `02 Split Holdout` does a quick stratified split of the main table (`components`) based on the presence or absence of any flood risk.
4. `03a` and `03b` do the sample tessellation, indexing, joins and basic feature engineering of all the Delta tables into a single feature table for the train and test datasets, respectively.
5. Once the `features_train` table is created, an AutoML experiment can be kicked off *(NB this requires an ML cluster)* via the UI.

