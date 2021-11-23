CREATE DATABASE simpledatastore;
--USE simpledatastore;
--for postgresql: \c simpledatastore

CREATE TABLE 
    IF NOT EXISTS 
playgroundpoints (
    fid VARCHAR(50) PRIMARY KEY, 
    objectid INT NOT NULL, 
    shape VARCHAR(50) NOT NULL, 
    anlname VARCHAR(50) NOT NULL, 
    bezirk INT NOT NULL, 
    spielplatzdetail VARCHAR(200) NOT NULL, 
    typdetail VARCHAR(200) NOT NULL, 
    seannocaddata VARCHAR(200) NOT NULL);