


--Environment from the architetual point of view OLTP,STAGING,EDW AND DATA MART
--From the ETL process, we are intrested in the Staging and EDW
--Packagetype---Dimension,Fact
--- Frequency of run---daily run,weekly,Endof month,yearly

Create Schema Control

create table control.Environment
(
		EnvironmentID int,
		Environment Nvarchar(50),
		constraint Control_Environment_pk primary key (EnvironmentID )

)

Insert into control.Environment(EnvironmentID,Environment)
Values
		(1, 'Staging'),
		(2, 'EDW')

create table control.PackageType
(
		PackageTypeID int,
		PackageType Nvarchar(50),
		constraint Control_PackageType_pk primary key (PackageTypeId)

)
Insert into Control.PackageType(PackageTypeID,PackageType)
Values
		(1,'Dimension'),
		(2,'Fact')

		use tescaControl
drop table control.Frequency

create table control.Frequency
(
		FrequencyID int,
		Frequency Nvarchar(50),
		constraint Control_Frequency_pk primary key (FrequencyId)

)
Insert into Control.Frequency(FrequencyID,Frequency)
Values
		(1,'Daily'),
		(2,'End of the Week'),
		(3,'End of the Month'),
		(4,'End of the Year'),
		(5,'Houly')
create table control.Package
(
		PackageID int ,
		PackageName Nvarchar(50),	---stgstore.dtsx
		SequenceNO int,  
		EnvironmentID int,
		PackageTypeID int,
		FrequencyID int,
		RunStartDate date,
		RunEndDate date,
		Active bit, ---0 to pause,1 to active
		lastRunDate datetime,
		constraint control_package_pk primary key(packageID),
		constraint control_package_environmentID_fk foreign Key(EnvironmentID) references control.environment(Environmentid),
		constraint control_package_PackageTypeID_fk foreign Key(PackageTypeID) references control.PackageType(PackageTypeID),
		constraint control_package_frequencyID_Fk foreign key (frequencyID) references control.frequency(frequencyid)

)



Alter table Control.package
drop constraint control_package_environmentID_fk 

Alter table control.package
drop constraint control_package_PackageTypeID_fk

Alter table control.package
drop constraint control_package_frequencyID_Fk


truncate table control.package
---OLTP to Staging => Stgcount = stgdescount
--- Staging to EDW => postcount = precount + currentcount + type2 count
--- Fact table staging to edw => postcount = precount + currentcount


create table control.metrics
(
		MetricID int identity(1,1),
		PackageID  int,---stgstore.dtsx
		stgSourceCount int,   --from OLTP
		stgDesCount int, ---to staging
		Precount int, --what data you have in EDW
		CurrentCount int,---current data u are loading from Stg into EDW
		Type1Count int,---
		Type2Count int,
		PostCount int, ---0 to pause,1 to active
		RunDate datetime,
		constraint control_MetricID_pk primary key(MetricID),
		constraint control_Metric_PackageID_fk foreign Key(PackageID) references control.Package(PackageID),
 
)

Alter table control.metrics
drop constraint control_Metric_PackageID_fk


declare @PackageID int=?
Declare @stgSourceCount int=?
Declare @stgDesCount int=?

insert into Control.metrics(PackageID,stgSourceCount,StgDesCount,RunDate)
values(@PackageID,@stgSourceCount,@stgDesCount,getdate())

Update Control.Package
Set lastRunDate=getdate() where PackageID=@PackageID



Update  control.package
set PackageName= lower(left(packagename,1)) + substring(packagename,2, len(packagename)-1);


insert into Control.Package(PackageID,PackageName,SequenceNO,EnvironmentID,PackageTypeID,FrequencyID,RunStartDate,Active)
Values

(1,'stgStore.dtsx',100,1,1,1,convert(date,GETDATE()),1),
(2,'stgProduct.dtsx',200,1,1,1,convert(date,GETDATE()),1),
(3,'stgPromotion.dtsx',300,1,1,1,convert(date,GETDATE()),1),
(4,'stgCustomer.dtsx',400,1,1,1,convert(date,GETDATE()),1),
(5,'stgPOSChannel.dtsx',500,1,1,1,convert(date,GETDATE()),1),
(6,'stgVendor.dtsx',600,1,1,1,convert(date,GETDATE()),1),
(7,'stgEmployee.dtsx',700,1,1,1,convert(date,GETDATE()),1),
(8,'stgMisconduct.dtsx',800,1,1,1,convert(date,GETDATE()),1),
(9,'stgDecison.dtsx',900,1,1,1,convert(date,GETDATE()),1),
(10,'stgAbsent.dtsx',1000,1,1,1,convert(date,GETDATE()),1),
(11,'stgSalesAnalysis.dtsx',1100,1,2,1,convert(date,GETDATE()),1),
(12,'stgPurchaseAnalysis.dtsx',1200,1,2,1,convert(date,GETDATE()),1),
(13,'stgOvertimeAnalysis.dtsx',1300,1,2,1,convert(date,GETDATE()),1),
(14,'stgMisconductAnalysis.dtsx',1400,1,2,1,convert(date,GETDATE()),1),
(15,'stgAbsentAnalysis.dtsx',1500,1,2,1,convert(date,GETDATE()),1)


select c.PackageID,c.PackageName from
(
select PackageID,PackageName ,SequenceNO  from Control.Package
where 
				Active=1 and RunStartDate<=convert(date,getdate()) AND 
				(RunEndDate IS NULL OR RunEndDate>=CONVERT(DATE, GETDATE()))
				AND EnvironmentID=1 AND FrequencyID=1 

Union all			

select PackageID,PackageName ,SequenceNO  from Control.Package
where 
				Active=1 and RunStartDate<=convert(date,getdate()) AND 
				(RunEndDate IS NULL OR RunEndDate>=CONVERT(DATE, GETDATE()))
				AND EnvironmentID=1 AND FrequencyID=2 
				AND DATEPART(DW,DATEADD(D,-1,CONVERT(DATE,GETDATE())))=7
Union all

select PackageID,PackageName ,SequenceNo  from Control.Package
where 
				Active=1 and RunStartDate<=convert(date,getdate()) AND 
				(RunEndDate IS NULL OR RunEndDate>=CONVERT(DATE, GETDATE()))
				AND EnvironmentID=1 AND FrequencyID=3 
				AND dateadd(day,-1,getdate())=EOMONTH(dateadd(day,-1,getdate()))
Union all

select PackageID,PackageName ,SequenceNO  from Control.Package
where 
				Active=1 and RunStartDate<=convert(date,getdate()) AND 
				(RunEndDate IS NULL OR RunEndDate>=CONVERT(DATE, GETDATE()))
				AND EnvironmentID=1 AND FrequencyID=4 
				AND dateadd(day,-1,getdate())=EOMONTH(dateadd(day,-1,getdate()))
				AND month(dateadd(day,-1,getdate()))=12

Union all

select PackageID,PackageName ,SequenceNO  from Control.Package
where 
				Active=1 and RunStartDate<=convert(date,getdate()) AND 
				(RunEndDate IS NULL OR RunEndDate>=CONVERT(DATE, GETDATE()))
				AND EnvironmentID=1 AND FrequencyID=5 
				And ((datepart(hour,dateadd(dd,-1,getdate()))*60+
datepart(minute,dateadd(dd,-1,getdate())))%60)=0
) C order by SequenceNO asc


									/*select (datepart(hour,dateadd(dd,-1,getdate()))*60+
									datepart(minute,dateadd(dd,-1,getdate())))%60

									select getdate(),DATEPART(HOUR,getdate())*60+datepart(minute,getdate()),
									(datepart(hour,getdate())*60+datepart(minute,getdate()))%60


									select convert(float,343)/convert(float,60) as divide, 343%60 as modu			

									Select getdate() times, datepart(minute,getdate()) min,datepart(hour,getdate()) hour,
									datepart(second,getdate()) sec
									*/

select * from Control.Package
select * from Control.metrics

ALTER TABLE control.package DROP CONSTRAINT FrequencyId;


--Dimension EDW Loading in the visual Studio

declare @PackageID int=?
Declare @PreCount int=?
Declare @CurrentCount int=?
Declare @Type1Count  int =?
declare @Type2Count int=?
Declare @PostCount int=?
insert into Control.metrics(PackageID,PreCount,CurrentCount,Type1Count,Type2Count,PostCount,RunDate)
values(@PackageID,@PreCount,@CurrentCount,@Type1Count,@Type2Count,@PostCount,getdate())

Update Control.Package
Set lastRunDate=getdate() where PackageID=@PackageID




Insert into Control.Package(PackageID,PackageName,SequenceNO,EnvironmentID,PackageTypeID,FrequencyID,RunStartDate,Active)
Values
(25,'dimAbsent.dtsx',2900,2,1,1,convert(date,getdate()),1),
(24,'dimDecision.dtsx',2800,2,1,1,convert(date,getdate()),1),
(23,'dimMisconduct.dtsx',2700,2,1,1,convert(date,getdate()),1),
(22,'dimEmployee.dtsx',2600,2,1,1,convert(date,getdate()),1),
(21,'dimVendor.dtsx',2500,2,1,1,convert(date,getdate()),1),
(20,'dimPOSChannel.dtsx',2400,2,1,1,convert(date,getdate()),1),
(19,'dimCustomer.dtsx',2300,2,1,1,convert(date,getdate()),1),
(18,'dimPromotion.dtsx',2200,2,1,1,convert(date,getdate()),1),
(17,'dimProduct.dtsx',2100,2,1,1,convert(date,getdate()),1),
(16,'dimStore.dtsx',2000,2,1,1,convert(date,getdate()),1),

delete from Control.Package where PackageID='17'


--Fact loading script

Create table Control.Anomalies
(AnomaliesSK int identity (1,1),
packageID int,
AttributeName Nvarchar(50),
AttributeData Nvarchar(50),
LoadDate datetime,
constraint control_Anomalies_Sk primary key(AnomaliesSK),
constraint control_anomalies_packageFk foreign key (packageID) references control.package(packageID)
)

declare @PackageID int=?
Declare @PreCount int=?
Declare @CurrentCount int=?
Declare @PostCount int=?
insert into Control.metrics(PackageID,PreCount,CurrentCount,PostCount,RunDate)
values(@PackageID,@PreCount,@CurrentCount,@PostCount,getdate())

Update Control.Package
Set lastRunDate=getdate() where PackageID=@PackageID



update control.package
set packagename= Lower(left(packagename,1)) + substring(packagename,2,len(packagename)-1)

Update control.Package
set PackageName='factAbsentAnalysis.dtsx'
where packageID=29


Insert into Control.Package(PackageID,PackageName,SequenceNO,EnvironmentID,PackageTypeID,FrequencyID,RunStartDate,Active)
Values
(30,'factMisconductAnalysis.dtsx',3500,2,2,1,convert(date,getdate()),1)
(29,'factAbsentAnalysis.dtsx',3400,2,2,1,convert(date,getdate()),1)
(28,'factOvertimeAnalysis.dtsx',3300,2,2,1,convert(date,getdate()),1)
(27,'factPurchaseAnalysis.dtsx',3200,2,2,1,convert(date,getdate()),1)
(26,'factSalesAnalysis.dtsx',3100,2,2,1,convert(date,getdate()),1)

-- Control Framework EDW
create procedure spPipelineEDW(@environment int)
as
Begin

set nocount on
select c.PackageID,c.PackageName from
(
select PackageID,PackageName ,SequenceNO  from Control.Package
where 
				Active=1 and RunStartDate<=convert(date,getdate()) AND 
				(RunEndDate IS NULL OR RunEndDate>=CONVERT(DATE, GETDATE()))
				AND EnvironmentID=@environment AND FrequencyID=1 

Union all			

select PackageID,PackageName ,SequenceNO  from Control.Package
where 
				Active=1 and RunStartDate<=convert(date,getdate()) AND 
				(RunEndDate IS NULL OR RunEndDate>=CONVERT(DATE, GETDATE()))
				AND EnvironmentID=@environment AND FrequencyID=2 
				AND DATEPART(DW,DATEADD(D,-1,CONVERT(DATE,GETDATE())))=7
Union all

select PackageID,PackageName ,SequenceNo  from Control.Package
where 
				Active=1 and RunStartDate<=convert(date,getdate()) AND 
				(RunEndDate IS NULL OR RunEndDate>=CONVERT(DATE, GETDATE()))
				AND EnvironmentID=@environment AND FrequencyID=3 
				AND dateadd(day,-1,getdate())=EOMONTH(dateadd(day,-1,getdate()))
Union all

select PackageID,PackageName ,SequenceNO  from Control.Package
where 
				Active=1 and RunStartDate<=convert(date,getdate()) AND 
				(RunEndDate IS NULL OR RunEndDate>=CONVERT(DATE, GETDATE()))
				AND EnvironmentID=@environment AND FrequencyID=4 
				AND dateadd(day,-1,getdate())=EOMONTH(dateadd(day,-1,getdate()))
				AND month(dateadd(day,-1,getdate()))=12

Union all

select PackageID,PackageName ,SequenceNO  from Control.Package
where 
				Active=1 and RunStartDate<=convert(date,getdate()) AND 
				(RunEndDate IS NULL OR RunEndDate>=CONVERT(DATE, GETDATE()))
				AND EnvironmentID=@environment AND FrequencyID=5 
				And ((datepart(hour,dateadd(dd,-1,getdate()))*60+
datepart(minute,dateadd(dd,-1,getdate())))%60)=0
) C order by SequenceNO asc
end

Execute spPipelineEDW 2

select * from Control.metrics M 
inner join Control.Package p on M.PackageID=p.PackageID
 where convert(date,lastRunDate)='2023-12-30'
 


select * from Control.Package 

select * from control.Anomalies where packageID=30

Truncate Table 

select (Precount+CurrentCount) as totalcount,PostCount,PackageID,RunDate	from Control.metrics
where PackageID='26'or PackageID = '27'or PackageID='28'or PackageID= '29' or PackageID= '30'


select (Precount+CurrentCount) as totalcount,PostCount,PackageID,RunDate	from Control.metrics
where PackageID='26'or PackageID = '27'or PackageID='28'or PackageID= '29' or PackageID= '30' and PostCount=(Precount+CurrentCount)
