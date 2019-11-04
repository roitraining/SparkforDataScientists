CREATE TABLE Regions(
RegionID int,
RegionName string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/home/student/ROI/Spark/datasets/northwind/CSV/regions' OVERWRITE INTO TABLE Regions;

select * from regions

