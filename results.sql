-- commands to create results database in RDS and using that to create PowerBI reports

create database results;
use results;

create table out_a( 
    key varchar(30),
    value float
);

LOAD DATA LOCAL INFILE 'path_to_output_a'
INTO TABLE out_a
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';



create table out_b( 
    key varchar(30),
    value float
);

LOAD DATA LOCAL INFILE 'path_to_output_b'
INTO TABLE out_b
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';



create table out_c( 
    key int,
    value int
);

LOAD DATA LOCAL INFILE 'path_to_output_c'
INTO TABLE out_c
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;



create table out_d( 
    key varchar(5),
    value float
);

LOAD DATA LOCAL INFILE 'path_to_output_d'
INTO TABLE out_d
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';



create table out_e( 
    key varchar(5),
    value float
);

LOAD DATA LOCAL INFILE 'path_to_output_e'
INTO TABLE out_e
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;



CREATE TABLE out_f(
    month INT,
    weekstatus VARCHAR(20),
    daystatus VARCHAR(10),
    Revenue float
);

LOAD DATA LOCAL INFILE 'path_to_output_f_transformed'
INTO TABLE out_f
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n';
