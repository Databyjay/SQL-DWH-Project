/*
----------------------------------------------------------------------------------------
Create Database & schemas
----------------------------------------------------------------------------------------
Script Purpose: 
    this scrpit creats a new database named DWH by checking whether its already existed or not if exists then it will be 
    dropped & recreated, then it will go creating schemas of 3 named bronze, silver & gold


  
  
  
  ---- Create Database 'DWH'

use master;
go

-- Drop and rec recreate the 'Datawarehouse; Database
if exists (select 1 from sys.databases where name = 'DWH')
begin
	alter database DWH set single_user with rollback immediate;
	Drop database DWH;
end;
go


--- create the 'DWH' Database

create database DWH;
go

use DWH;
go

-- create schemas
create schema Bronze;
go
create schema Silver;
go
create schema Gold;
