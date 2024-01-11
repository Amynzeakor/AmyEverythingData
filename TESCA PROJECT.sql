USE master
GO

create database tescaOLTP

create database tescaStaging

create database tescaEDW

create database tescaControl

use tescaoltp

select * from SalesTransaction
order by TransDate asc

select * from PurchaseTransaction

--schema creation

use tescaEDW
create schema EDW
go
use tescastaging

create schema retail
go
create schema HR
go
------- loading staging
use tescaOLTP
select s.StoreID,s.StoreName,s.StreetAddress,
c.CityName,st.State,getdate() as loaddate from Store S
inner join City C on s.CityID=c.CityID
inner join state St on c.StateID=st.StateID

select count(*) as stgSourceCount from Store s
inner join City C on s.CityID=c.CityID
inner join state St on c.StateID=st.StateID

use tescaStaging

select * from retail.store


if OBJECT_ID('retail.store') is not null
truncate table retail.store

create table retail.store
(
StoreID int,
StoreName nvarchar(50),
StreetAddress nvarchar(50),
CityName nvarchar(50),
state nvarchar(50),
LoadDate datetime default getdate(),
constraint pk_retail_store primary key(StoreID)
)

--loading store EDW
use tescaStaging
select StoreID,StoreName,StreetAddress,CityName,state from retail.store

--Precount Count 

use tescaEDW

---Before loading
select count(*)as PreCount from edw.dimstore

--After loading EDW
select count(*)as PostCount from edw.dimstore

select * from EDW.dimstore

create table EDW.dimstore
(
StoreSK int identity (1,1),
StoreID int,
StoreName nvarchar(50),
StreetAddress nvarchar(50),
CityName nvarchar(50),
state nvarchar(50),
EffectiveStartDate datetime ,
constraint EDW_dimstore_SK primary key (StoreSK)
)
--product
---loading staging

use tescaOLTP
select p.productid,p.ProductNumber,p.Product,d.Department,p.UnitPrice
from Product p
inner join Department D on p.DepartmentID=D.DepartmentID

--Source Count
select count(*)from 
Product p inner join Department D on p.DepartmentID=D.DepartmentID

use tescaStaging

if object_id('retail.product') is  not null
truncate table retail.product

create table retail.product
(
ProductID int,
ProductNumber nvarchar(50),
Product nvarchar(50),
department Nvarchar(50),
unitprice float,
loaddate datetime default getdate(),
constraint retail_product_pk primary key (productID)

)
Select * from retail.product

---Loading into EDW
use tescaStaging
select ProductID,ProductNumber,product,department,unitprice from retail.product 

use tescaEDW

select count(*) as PreCount from EDW.dimproduct
select count(*) as PostCount from EDW.dimproduct
select * from EDW.dimproduct

create table EDW.dimproduct
(
ProductSK int identity(1,1),
ProductID int,
ProductNumber nvarchar(50),
Product nvarchar(50),
department Nvarchar(50),
unitprice float,
EffectiveLoadDate datetime ,
EffectiveEndDate datetime,
constraint EDW_dimproduct_SK primary key (ProductSk)

)
--loading promotion tescastaging
use tescaOLTP

select p.PromotionID,t.Promotion,p.StartDate as PromotionStartDate,p.EndDate as promotionEndDate,p.DiscountPercent,
getdate() as loadDate from Promotion P
inner join PromotionType t on p.PromotionTypeID=t.PromotionTypeID

--Data Source Count
select count(*) from Promotion P
inner join PromotionType t on p.PromotionTypeID=t.PromotionTypeID


use tescaStaging
if OBJECT_ID('retail.promotion') is not null
truncate table retail.promotion

create table retail.promotion
(
PromotionID int,
Promotion Nvarchar(50),
PromotionStartDate date,
PromotionEndDate date,
DiscountPercent float,
LoadDate datetime default getdate(),
constraint retail_promotion_pk primary key (promotionID)

)

--Loading Dimpromotion

use tescaStaging
select PromotionID,promotion,promotionStartDate,promotionEndDate,
DiscountPercent from retail.promotion

use tescaEDW

select count(* )PreCount from edw.dimpromotion
select count(*) as PostCount from EDW.dimpromotion
select * from edw.dimpromotion


create table EDW.dimpromotion
(
PromotionSK int identity (1,1),
PromotionID int,
Promotion Nvarchar(50),
PromotionStartDate date,
PromotionEndDate date,
DiscountPercent float,
EffectiveStartDate datetime
constraint EDW_dimpromotion_SK primary key (promotionsk)

)
--Loading Customer into staging
use tescaOLTP

--E-> Extract T-> Transform L-> Load
-- business Rule; Combine Lastname and Firstname, then Lastname comes first in upperlettrs and seperate with coma into the staging area

select c.CustomerID,CONCAT_WS(', ',upper(c.LastName),c.FirstName) CustomerName,c.CustomerAddress ,
ct.cityname , s.State,getdate() as LoadDate from Customer C
inner join city Ct on c.CityID=ct.CityID
inner join State S on ct.StateID=s.StateID


--Data count

select  Count(*) as stgSourceCount from Customer C
inner join city Ct on c.CityID=ct.CityID
inner join State S on ct.StateID=s.StateID

Use tescaStaging

IF OBJECT_ID('retail.customer') is not null
truncate table retail.customer
create table retail.customer
(
CustomerID int,
CustomerName nvarchar (250),
CustomerAddress nvarchar (50),
CityName nvarchar(50),
state nvarchar (50),
loadDate datetime default getdate(),
constraint retail_customer_PK primary key(customerID)
)

---loading into EDW
use tescaStaging

select CustomerID,CustomerName,CustomerAddress,CityName,state from retail.customer

Use tescaEDW
select count(*) as PreCount from EDW.dimcustomer
select count(*) as PostCount from EDW.dimcustomer
select * from edw.dimcustomer

use tescaEDW
exec sp_rename 'edw.dimcustomer.custometaddress','CustomerAddress','column'
create table EDW.dimcustomer
(
CustomerSK int identity(1,1),
CustomerID int,
CustomerName nvarchar (250),
CustometAddress nvarchar (50),
CityName nvarchar(50),
state nvarchar (50),
EffectiveStartDate date,
constraint EDW_Dimcustomer_SK primary key (CustomerSk)
)

--laoding POS into staging
use tescaOLTP
select p.ChannelID,p.ChannelNo,p.DeviceModel,p.InstallationDate,
p.SerialNo,getdate() as LoadDate from POSChannel P

--Data Source Count
select Count(*) from POSChannel P

USE tescaStaging

IF OBJECT_ID('retail.Poschannel')is not null
truncate table retail.Poschannel

create table retail.POSchannel
(
ChannelID int,
ChannelNo nvarchar(50),
DeviceModel nvarchar(50),
InstallationDate date,
serialNo nvarchar(50),
LoadDate datetime default getdate(),
constraint retail_POSchannel_PK primary key(ChannelID)
)

select * from retail.POSchannel
--Loading into the EDW

use tescaStaging

select ChannelID,ChannelNo,DeviceModel,InstallationDate,serialNo from retail.POSchannel

select count(*) as Precount from edw.dimposchannel
select count(*) as PostCount from edw.dimposchannel

use tescaEDW

create table EDW.DimPOSchannel
(
ChannelSK int identity(1,1),
ChannelID int,
ChannelNo nvarchar(50),
DeviceModel nvarchar(50),
InstallationDate date,
serialNo nvarchar(50),
EffectiveStartDate date,
EffectiveEndDate date,
constraint EDW_dimPOSchannel_SK primary key(ChannelSK)
)

use tescaOLTP
-- business Rule; Combine Lastname and Firstname, then Lastname comes first in upperlettrs and seperate with coma into the staging area

select v.VendorID,V.VendorNo,concat_ws(', ',upper(V.LastName),V.FirstName) VendorName,
V.RegistrationNo,V.VendorAddress ,c.CityName,S.State,getdate() as Loadate from Vendor V
inner join City C on V.CityID=c.CityID
inner join State S on C.StateID=S.StateID
  
  -- Data Source Count
  
select count(*) as stgSourceCount from Vendor V
inner join City C on V.CityID=c.CityID
inner join State S on C.StateID=S.StateID

  USE tescaStaging

  select * from retail.Vendor

  if Object_id('retail.Vendor') is not null
  truncate table retail.vendor

  create table retail.Vendor
  (
  VendorID int,
  VendorNO nvarchar (50),
 VendorName nvarchar(250),
  RegistrationNo nvarchar(50),
  VendorAddress nvarchar(50),
  CityName nvarchar(50),
  State nvarchar(50),
  LoadDate datetime default getdate(),
  constraint retail_vendor_pk primary key (VendorID)
  )

  ---Loading into the EDW
USE tescaStaging

select VendorID,VendorNO,VendorName,RegistrationNo,VendorAddress,
CityName,State from retail.Vendor

select count(*) as PreCount from edw.dimvendor
select count(*) as PostCount from edw.dimvendor
USE tescaEDW

create table EDW.DimVendor
  (
  VendorSK int identity(1,1),
  VendorID int,
  VendorNO nvarchar (50),
 VendorName nvarchar(250),
  RegistrationNo nvarchar(50),
  VendorAddress nvarchar(50),
  CityName nvarchar(50),
  State nvarchar(50),
  EffectiveStarDate datetime,
  EffectiveEndDate datetime,
  constraint EDW_Dimvendor_Sk primary key (VendorSK)
  )

  --Loading Employee into the staging 
  USE tescaOLTP
  -- business Rule; Combine Lastname and Firstname, then Lastname comes first in upperlettrs and seperate with coma into the staging area
--Change DOB to DateofBirth

  select E.EmployeeID,e.EmployeeNo,concat_ws(', ',upper(e.LastName),e.FirstName) as EmployeeName,
  e.DoB as DateofBirth,m.MaritalStatus,getdate() as LoadDate from Employee E
  inner join MaritalStatus M on M.MaritalStatusID=E.MaritalStatus

  --Source Data Count
  select count(*)from Employee E
  inner join MaritalStatus M on M.MaritalStatusID=E.MaritalStatus

  USE tescaStaging

  select * from retail.Employee
  if OBJECT_ID('retail.employee') is not null
  truncate table retail.employee
  
  create table retail.Employee
  (
  EmployeeID int,
  EmployeeNO nvarchar(50),
  EmployeeName nvarchar(250),
  DateofBirth date,
  MaritalStatus nvarchar (50),
  LoadDate datetime default getdate(),
  constraint retail_Employee_PK primary key (EmployeeID)
  )

  --Loading into EDW

  use tescaStaging
  select EmployeeID,EmployeeNO,EmployeeName,DateofBirth,MaritalStatus from retail.Employee
  
  select count(*) as PreCount from EDW.dimemployee
  select count(*) as PostCount from EDW.dimemployee

  Use tescaEDW
  
  create table EDW.DimEmployee
  (
  EmployeeSk int identity(1,1),
  EmployeeID int,
  EmployeeNO nvarchar(50),
  EmployeeName nvarchar(250),
  DateofBirth date,
  MaritalStatus nvarchar (50),
 EffectiveStartDate datetime,
 EffectiveEndDate datetime,
  constraint EDW_DIMEmployee_SK primary key (EmployeeSK)
  )



  --misconduct---
  --this will be loaded from the CSV file into the staging

  USE tescaStaging
  If OBJECT_ID('hr.misconduct') is not null
  truncate table hr.misconduct
   
 create table HR.Misconduct
  (
  ID int identity(1,1),
	  MisconductID int,
	  MisconductDesc nvarchar (250),
	  LoadDate datetime default getdate()
	  constraint HR_Misconduct_Pk primary key(ID)
  )

  select * from HR.Misconduct
  truncate table HR.Misconduct
  --loading into EDW

USE tescaStaging
--aproach 1
select distinct MisconductID,MisconductDesc from hr.Misconduct

-- apprach 2
select MisconductID,MisconductDesc from hr.Misconduct
group by MisconductID,MisconductDesc

--3 if one of the misconduct attribute change?
--introduce surrogate key id

select MisconductID,MisconductDesc from hr.Misconduct
where ID in(select MAX(id) from hr.Misconduct group by MisconductID)

select * from hr.Misconduct

SELECT * FROM HR.MISCONDUCT WHERE ID NOT IN  (select MAX(ID) from hr.Misconduct GROUP BY MisconductID);

  select count(*) as PreCount from EDW.DimMisconduct
  select count(*) as PostCount from EDW.DimMisconduct

use tescaEDW

create table EDW.DimMisconduct
  (
  MisconductSK int identity(1,1),
	  MisconductID int,
	  MisconductDesc nvarchar (250),
	  EffectiveStartDate datetime
	  constraint EDW_DimMisconduct_sk primary key(Misconductsk)
  )

  use tescaStaging

  select * from Hr.Decision

  if OBJECT_ID('HR.Decision') is not null
  truncate table HR.Decision

  create table HR.Decision
  (
  DecisionID int,
  Decision Nvarchar (250),
  LoadDate datetime default getdate(),
  constraint HR_Decision_pk primary key(decisionID)

  )

  --loading to EDW
  use tescaStaging
  select DecisionID,Decision from HR.Decision

  use tescaEDW
select count(*) as PreCount from EDW.DimDecision
select count(*) as PostCount from EDW.DimDecision


  use tescaEDW
  create table EDW.DimDecision
  (
  DecisionSK int identity(1,1),
  DecisionID int,
  Decision Nvarchar (250),
 EffectiveStartDate datetime,
  constraint EDW_DimDecision_sk primary key(decisionSK)

  )

--LOADING ABSENT DATA
use tescaStaging
if OBJECT_ID('HR.Absent') is not null
truncate table HR.Absent

create table HR.Absent
(CategoryID int,
Category nvarchar(250),
LoadDate datetime default getdate(),
constraint HR_Absent_pk primary key(categoryid)
)

--lOADING EDW
use tescaStaging
select CategoryID,Category from hr.Absent

use tescaEDW

select count(*) as PreCount from EDW.DimAbsent
select count(*) as PostCount from EDW.DimAbsent

create table EDW.DimAbsent
(

Categorysk int identity(1,1),
CategoryID int,
Category nvarchar(250),
EffectiveStartDate datetime,
constraint EDW_DimAbsent_sk primary key(categorysk)
)

use tescaoltp
select * from SalesTransaction

---Time transformation Rule
--0-23hrs  
-- periodoftheday 0-mid night,1-4 early hour,5-11 morning, 12 noon, 13-17 afternoon,
---- 18-20 evening,21-23 night
--Bussinesshour  0-6 closed, 7-18 open,19-23 closed
--Weekenddayhour  0-2 closed, 3-21 open,22-23 closed

use tescaEDW
create table EDW.dimtime
(timeSK int identity(1,1),
dayhour int,
dayperiod nvarchar(50),
dailydayhour nvarchar(50),
weekenddayhour nvarchar(50),
EffectiveStartDate datetime,
constraint EDW_dimtime_SK primary key(timesk)
)

alter procedure EDW.spTimeGenerator
as
begin
set nocount on;

IF OBJECT_ID('edw.dimtime') is not null
truncate table EDW.dimtime

declare @starthour int=0
  while @starthour<=23
	begin	insert into edw.dimtime(dayhour,dayperiod,dailydayhour,weekenddayhour,EffectiveStartDate)
		select @starthour as dayhour,
			case
				when @starthour=0 then 'Mid Night'
				when @starthour>=1 and @starthour<=4 then 'Early Hour'
				when @starthour>=5 and @starthour <=11 then 'Morning'
				when @starthour= 12 then 'Noon'
				when @starthour>=13 and @starthour<=17 then 'Afternoon'
				when @starthour>=18 and @starthour<=20 then 'Evening'
				when @starthour>=21 and @starthour<=23 then 'Night'
		end as Dayperiod,
		case
				when @starthour>=0 and @starthour<=6 then 'Closed'
				when  @starthour>=7 and @starthour<=18 then 'Open'
				when @starthour>=19 and @starthour<=23 then 'Closed'
		End as Dailyhour,

		case	
				when @starthour>=0 and @starthour<=2 then 'Closed'
				when  @starthour>=3 and @starthour<=21 then 'Open'
				when @starthour>=22 and @starthour<=23 then 'Closed'
		End as Weekenddayhour,
		getdate() as EffectiveStartDate
	
	 select @starthour=@starthour+1
	end
End

exec EDW.spTimeGenerator

Select * from EDW.Dimtime

---DimDate
--How do i determine my start date, this will be defined by the business
--date format, How does the business want to see the date
--enddate could be futuristic date;it can also be stated by the business

-- to establish the base year;
--base year 2013-01-01
--the last year could be a futuristic date determined by the Business

Use tescaEDW

Create table EDW.DimDate
(
datekey int,
businessDate date,
BusinessYear Int,
BusinessMonth int,
BusinessDay int,
EnglishMonth Nvarchar (50),
EnglishDayofWeek Nvarchar (50),
BusinessQuater nvarchar(2),  
FrenchMonth nvarchar(50),
FrenchDayofWeek nvarchar(50),
EffectiveStartDate datetime,
constraint EDW_Dimdate_sk primary key (datekey)
)

create procedure EDW.spGenerateCalender (  @generateYear int)
as
Begin
--declare @generateYear int=70
set nocount on
IF object_id ('EDW.Dimdate') is not null
truncate table edw.dimdate
declare @startDate date=
(
select min(convert(date,transdate)) from tescaOLTP.dbo.SalesTransaction
		union
select min(convert(date,transdate)) from tescaOLTP.dbo.PurchaseTransaction
)

declare @enddate date

select @startDate=DATEADD(year,-1,@startdate)
select @enddate=DATEADD(year,@generateYear,datefromparts(year(getdate()),12,31))
declare @nofodays int=datediff(day,@startdate,@enddate)
declare @currentday int=0
--select @startDate,@enddate,@nofodays
declare @currentdate date
While @currentday<=@nofodays
Begin
	select @currentdate=DATEADD(day,@currentday,@startdate)
	
insert into EDW.dimdate(datekey,businessdate,businessyear,businessmonth,businessday,
englishmonth,englishdayofweek,businessquater,frenchmonth,frenchdayofweek,effectivestartdate)
	select convert(int,convert(nvarchar(8),@currentdate,112)) as DateKey,@currentdate as Bussinessdate,
	year(@currentdate)as BusinessYear,datepart(month,@currentdate)as BusinessMonth,day(@currentDate) as BusinessDay,
	datename(month,@currentdate) englishmonth, datename(DW,@currentdate) as englishdayoftheweek,concat('Q',datepart(q,@currentdate)) BusinessQuater,
	
	case datepart(month,@currentdate)
	when 1 then 'Janvier' when 2 then 'fevrier' when 3 then 'Mars' when 4 then 'Avril'
	when 5 then 'Mai' when 6 then 'Juin' when 7 then 'Juillet' when 8 then 'aout'
	when 9 then'Septembre' when 10 then 'Octobre' when 11 then 'Novembre' when 12 then 'Decembre'
	end as 'FrenchMonth',

	case datepart(Weekday,@currentdate)
	when 1 then 'Dimanche' when 2 then 'lundi' when 3 then 'Mardi' when 4 then 'Mercredi'
	when 5 then 'Jeudi' when 6 then 'Vendredi' when 7 then 'Samedi' 
	End as FrenchDay,

	getdate() as EffectiveStartDate
	
	select @currentday=@currentday+1

End
END

Exec EDW.spGenerateCalender 100

select * from EDW.dimdate

select min(businessdate),max(businessdate) from EDW.dimdate

---sales analysis
use tescaOLTP

select min(transdate),max(transdate) from salestransaction

select * from salestransaction where convert(date,transdate)='2023-09-17'

---shifted the dates table to have futuristic date
UPDATE	salestransaction
SET		transdate=dateadd(year,2,transdate),
		orderdate=dateadd(year,2,orderdate),
		deliverydate=dateadd(year,2,deliverydate)

	--Staging tables
use tescaOLTP


--for previous unloaded data or populating the EDW for the first time and contineous
Use tescaOLTP
IF	   (select Count(*) from tescaEDW.EDW.fact_salesAnalysis) =0
		select transactionID,transactionNo,convert(date,transdate) as transdate,
		datepart(hour,transdate) transhour,convert(date,orderdate) orderdate,
		datepart(hour,orderdate) orderhour, convert(date,deliverydate) deliverydate,
		channelid,employeeid,productid,storeid,promotionid,CustomerID,
		quantity,lineamount from salestransaction where CONVERT (date,transdate)<= CONVERT(date,dateadd(dd,-1,GETDATE()))

Else
		select transactionID,transactionNo,convert(date,transdate) as transdate,
		datepart(hour,transdate) transhour,convert(date,orderdate) orderdate,
		datepart(hour,orderdate) orderhour, convert(date,deliverydate) deliverydate,
		channelid,employeeid,productid,storeid,promotionid,CustomerID,
		quantity,lineamount from salestransaction where CONVERT (date,transdate)= CONVERT(date,dateadd(dd,-1,GETDATE()))


--- getting the Source count
IF  (select count(*) from tescaEDW.EDW.Fact_SalesAnalysis)=0
	select count(*) as stgSourceCount from salestransaction where convert(date,transdate)<=convert(date,dateadd(dd,-1,getdate()))
Else
	select count(*) as stgSourceCount from salestransaction where convert(date,transdate)=convert(date,dateadd(dd,-1,getdate()))


 if OBJECT_ID('retail.salestransaction') is not null
Truncate table retail.salestransaction

	  create table retail.salestransaction
	  (
	  TransactionID int,
	  TransationNo nvarchar(50),
	  TransDate date,
	  Transhour int,
	  Orderdate date,
	  Orderhour int,
	  Deliverydate date,
	  ChannelID int,
	  CustomerID int,
	  EmployeeID int,
	  ProductID int,
	  StoreID int,
	  PromotionID int,
	  Quantity int,
	  LineAmount float,
	  LoadDate Datetime
	  constraint retail_salestransaction_pk primary key (transactionID)
	  )

	  --Loading Fact table from EDW.Staging

select TransactionID ,TransationNo,TransDate,Transhour,Orderdate, Orderhour, 
Deliverydate,ChannelID,CustomerID,EmployeeID,ProductID,StoreID, PromotionID,  
Quantity, LineAmount,getdate() as LoadDate from retail.salestransaction

select count(*) as PreCount from EDW.Fact_SalesAnalysis
select count(*) as PostCount from EDW.Fact_SalesAnalysis

use tescaEDW

select * from edw.fact_salesanalysis
alter table edw.fact_salesanalysis alter column transactionno int
create table EDW.Fact_SalesAnalysis
(
	SalesAnalysisSk int identity (1,1),
	TransactionNO Int,
	TransdateSk int,
	TransHourSk int,
	OrderdateSK int,
	OrderHourSk int,
	DeliveryDateSK int,
	ChannelIDSK int,
	CustomerSk int,
	SalesPersonSk int,
	Productsk int,
	StoreSk int,
	PromotionSk int,
	Quantity float,
	LineAmount  Float,
	LoadDate datetime,
	 
constraint EDW_fact_salesAnalysis_SK primary key (SalesAnalysisSk ),
constraint fact_salesAnalysis_transdate_SK foreign key (TransdateSk ) references edw.dimdate(datekey),
constraint fact_salesAnalysis_transtime_SK foreign key (TranshourSk ) references edw.dimtime(TimeSk),
constraint fact_salesAnalysis_Orderdate_SK foreign key (orderdateSk ) references edw.dimdate(datekey),
constraint fact_salesAnalysis_OrderHour_SK foreign key (TransdateSk ) references edw.dimTime(Timesk),
constraint fact_salesAnalysis_Deliverydate_SK foreign key (DeliverydateSk ) references edw.dimdate(datekey),
constraint fact_salesAnalysis_ChannelID_SK foreign key (ChannelIDSk ) references edw.DimPosChannel(ChannelSk),
constraint fact_salesAnalysis_CustomerID_SK foreign key (CustomerSk ) references edw.dimCUSTOMER(CUSTOMERSk),
constraint fact_salesAnalysiss_SalesPerson_SK foreign key (SalesPersonSk ) references edw.dimEmployee(EmployeeSk),
constraint fact_salesAnalysis_product_SK foreign key (productsk ) references edw.dimproduct(productsk),
constraint fact_salesAnalysis_Store_SK foreign key (Storesk ) references edw.dimStore(Storesk),
constraint fact_salesAnalysis_promotion_SK foreign key (promotionsk ) references edw.dimpromotion(promotionsk),
)

alter table edw.fact_salesanalysis drop constraint fact_salesAnalysis_OrderHour_SK
alter table edw.fact_salesanalysis add constraint fact_salesAnalysis_orderhour_SK foreign key(OrderHourSk) references edw.dimtime(timesk)

--Loading PurchaseAnalysis

use tescaOLTP
select Min(transdate),Max(transdate) from purchasetransaction
select * from purchasetransaction where convert(date,transdate)='2023-11-20'


--SHIFTING DATA FOR 2 YEARS

update purchasetransaction
set
     transdate=dateadd(year,2,transdate),
	 orderdate=dateadd(year,2,orderdate),
	 Deliverydate=dateadd(year,2,deliverydate),
	 shipdate=dateadd(year,2,shipdate)

	 ---From the Beginning of till N-1
	 Use tescaOLTP

IF	   (select count(*) from tescaedw.EDW.Fact_PurchaseAnalysis) >0

		select TransactionID,TransactionNO,convert(date,transdate) Transdate,convert(date,orderdate) Orderdate,convert(date,deliverydate) Deliverydate,convert(date,Shipdate) Shipdate,VendorID,EmployeeId,ProductID,StoreId,
		Quantity,Lineamount,datediff(day,orderdate,deliverydate)+1 as Diffdate,getdate() as LoadDate from purchasetransaction
		where convert(date,transdate)=dateadd(dd,-1,convert(date,getdate()))

 Else

		select TransactionID,TransactionNO,convert(date,transdate) Transdate,convert(date,orderdate) Orderdate,convert(date,deliverydate) Deliverydate,convert(date,Shipdate) Shipdate,VendorID,EmployeeId,ProductID,StoreId,
		Quantity,Lineamount,datediff(day,orderdate,deliverydate)+1 as Diffdate ,getdate() as LoadDate from purchasetransaction
		where convert(date,transdate)<=dateadd(dd,-1,convert(date,getdate()))

	--Source Count
	If (select count(*) from tescaEDW.EDW.Fact_PurchaseAnalysis)>0
	select count(*) as stgSourceCount from purchasetransaction where convert(date,transdate)=convert(date,dateadd(day,-1,getdate()))
	ELSE
	select count (*) as stgSourceCount from purchasetransaction where convert(date,transdate)<= convert(date,dateadd(day,-1,getdate()))

Truncate table tescaEDW.EDW.fact_Overtimeanalysis
select count(*) from tescaEDW.EDW.Fact_PurchaseAnalysis
select count(*) from tescaEDW.EDW.fact_OvertimeAnalysis
	select * from retail.PurchaseAnalysis 
		select * from retail.salestransaction
		select * from[dbo].[PurchaseTransaction]
		where convert(date,transdate) >= '2023-11-01'
	select count(*) from purchasetransaction where date


Use tescaStaging


IF OBJECT_ID(' retail.PurchaseAnalysis') is not null
truncate table  retail.PurchaseAnalysis

/*alter table retail.purchaseanalysis alter column transactionno nvarchar(50)
alter table retail.purchaseanalysis add LoadDate datetime default getdate()*/ --this caused an ERROR IN SSIS
create table retail.PurchaseAnalysis
(TransactionID int,
TransactionNO Nvarchar(50),
Transdate date,
Orderdate date,
Deliverydate date,
Shipdate date,
VendorID int,
EmployeeId int,
ProductID int,
StoreId int,
Quantity float,
LineAmount float,
Diffdate int,
LoadDate datetime default getdate()
constraint retail_purchaseAnalysis_pk primary key (transactionid)
)


Select TransactionID,TransactionNO,Transdate,Orderdate,Deliverydate,Shipdate
,vendorid,ProductID employeeid,storeid,quantity,lineamount,diffdate,getdate() as LoadDate from retail.PurchaseAnalysis

select count(*) as PreCount from edw.fact_purchaseAnalysis
select count(*) as PostCount from edw.fact_purchaseAnalysis
select * from edw.fact_purchaseAnalysis
select * from edw.fact_salesAnalysis
use tescaEDW

alter table edw.fact_purchaseAnalysis alter column 	TransactionNO nvarchar(50)
truncate table EDW.Fact_PurchaseAnalysis
create table EDW.Fact_PurchaseAnalysis
(
	purchaseAnalysisSk int identity (1,1),
	TransactionNO Int,
	TransdateSk int,
	OrderdateSK int,
	DeliveryDateSK int,
	ShipDateSk int,
	VendorSk int,
	PurchaserSk int,
	Productsk int,
	StoreSk int,
	Quantity float,
	LineAmount  Float,
	DiffDays int,
	LoadDate datetime,
	 
constraint EDW_fact_PurchaseAnalysis_SK primary key (purchaseAnalysisSk ),
constraint fact_PurchaseAnalysis_transdate_SK foreign key (TransdateSk ) references edw.dimdate(datekey),
constraint fact_purchaseAnalysis_Orderdate_SK foreign key (orderdateSk ) references edw.dimdate(datekey),
constraint fact_PurchaseAnalysis_Deliverydate_SK foreign key (DeliverydateSk ) references edw.dimdate(datekey),
constraint fact_purchaseAnalysis_Shipdate_SK foreign key (ShipdateSk ) references edw.dimdate(datekey),
constraint fact_PurchaseAnalysis_Vendor_SK foreign key (vendorSk ) references edw.dimvendor(vendorsk),
constraint fact_purchaseAnalysiss_Purchaser_SK foreign key (purchasersk ) references edw.dimEmployee(EmployeeSk),
constraint fact_PurchaseAnalysis_product_SK foreign key (productsk ) references edw.dimproduct(productsk),
constraint fact_PurchaseAnalysis_Store_SK foreign key (Storesk ) references edw.dimStore(Storesk),
)

--Loading Overtime data

--OvertimeId,EmployeeNo,Firstname,LastName,StartOverTime,EndOverTime
 
 use tescaStaging
 
 If OBJECT_ID('hr.overtime') is not null
 Truncate table hr.overtime

 --Duplicate data was found on the data compromsing the integrity of
 --primary Key. we need to remove the OVERTIMEID as primarykey by adding 
 --a new surrogate key. drop table and recreate it.
 -- Business rule use first record as the right record

--Drop table Hr.OverTime

Create table Hr.OverTime
 (
 OvertimeSk int identity(1,1),
 OvertimeID int,
 EmployeeNo Nvarchar(50),
 Firstname Nvarchar (50),
 LastName Nvarchar(50),
 StartOverTime Datetime,
 EndOverTime datetime,
 LoadDate datetime default getdate(),
 constraint Hr_OvertimeSk_Pk primary key (OvertimeSk)
 )
 --loading into EDW

 -- date that will be loaded will be from beginining till N-1 or N-1 date

 --This is the original query when loading into EDW if the data doest have duplicate
 select EmployeeNo,convert(date,startovertime) StartOvertimeDate, datepart(hour,StartOverTime) StartOvertimeHour ,
 convert(date,Endovertime) EndOvertimeDate, datepart(Hour,EndOverTime) as EndOvertimeHour from hr.OverTime


 --the Query below is for loading the edw when there is Duplicate in the data .

 select EmployeeNo,convert(date,startovertime) StartOvertimeDate,datepart(hour,StartOverTime ) StartOvertimeHour, convert(date,EndOverTime) EndOvertimeDate,
datepart(Hour,EndOverTime) EndOvertimeHour   from hr.OverTime Where OvertimeSk in (select Min(OvertimeSk) from hr.OverTime group by OvertimeId,EmployeeNo,convert(date,startovertime))
 --Loading into the EDW

 Use tescaEDW

 Select count(*) as edwCount from EDW.fact_OvertimeAnalysis
  Select count(*) as PreCount from EDW.fact_OvertimeAnalysis
 Select count(*) as PostCount from EDW.fact_OvertimeAnalysis

drop table EDW.fact_OvertimeAnalysis

 create table EDW.fact_OvertimeAnalysis
 (
	 OvertimeAnalysisSk int identity (1,1),
	 EmployeeSk int,
	 StartOvertimeDateSk int,
	 StartOvertimeHourSk int,
	 EndOvertimeDateSk int,
	 EndOvertimeHourSk int,
	 TotalHour int,
	 LoadDate datetime,
	constraint fact_OvertimeAnalysis_sk primary key(Overtimeanalysissk),
	constraint fact_OvertimeAnalysis_Employee_SK foreign key (employeesk ) references edw.dimEmployee(EmployeeSk),
	constraint fact_OvertimeAnalysis_Startovertimedate_SK foreign key (startovertimedateSk ) references edw.dimdate(datekey),
	constraint fact_OvertimeAnalysis_startOvertimehour_SK foreign key (startovertimehourSk ) references edw.dimtime(TimeSk),
	constraint fact_OvertimeAnalysisAnalysis_EndOvertimedate_SK foreign key (EndOvertimedateSk ) references edw.dimdate(datekey),
	constraint fact_OvertimeAnalysis_EndOvertimehour_SK foreign key (Endovertimehoursk ) references edw.dimtime(timesk),
 )


 -- Loading absent data---
 --EmpID,Store,absent_date,absent_hour,absent_category

 use tescaStaging

 IF OBJECT_ID(' HR.AbsentAnalysis') is not null
 truncate table  HR.AbsentAnalysis

 Create table HR.AbsentAnalysis
 (
	 AbsentID int identity(1,1),
	 EmpID int,
	 Store int,
	 Absent_date Date,
	 Absent_category int,
	 Absent_hour int,
	 Loaddate datetime
	 constraint Hr_AbsentAnalysis_absentId_PK primary  key(absentid)
 )

 -- Bus requirement states that the first entry is the right date to retain
 -- NI-1 and begining to N-1  needed to be taken care if in the ETL pipeline processing


 -- to De-DUPLICATE DATA
select	EmpID,Store,Absent_date,Absent_category,Absent_hour   from HR.AbsentAnalysis
where absentid in 
		 (select min(absentid)   from HR.AbsentAnalysis
group by AbsentID,EmpID,Store,Absent_date,Absent_category)

select *from HR.AbsentAnalysis
--Loading edw
Use tescaEDW
--Count EDW
select count(*) as edwCount from EDW.fact_absentAnalysis
select count(*) as PreCount from EDW.fact_absentAnalysis
select count(*) as PostCount from EDW.fact_absentAnalysis

create Table EDW.fact_absentAnalysis
(
	AbsentAnalysisSk int identity(1,1),
	EmployeeSk int,
	storeSK int,
	Absent_dateSk int,
	Absent_categorySk int,
	Absent_HourSk int,
	LoadDate datetime,



	constraint Fact_Absentanalysis_sk primary key (absentanalysissk),
	constraint Fact_Absentanalysis_EmployeeSk foreign Key (EmployeeSK) references EDW.Dimemployee(Employeesk),
	constraint Fact_Absentanalysis_StoreSk foreign key (StoreSK) references EDW.dimstore(storesk),
	constraint Fact_Absentanalysis_Absent_dateSk foreign key (absent_datesk) references EDW.dimdate(datekey),
	constraint Fact_Absentanalysis_Absent_categorySk foreign key (Absent_categorySk) references EDW.dimAbsent(categorysk)

)

exec sp_rename 'EDW.fact_absentAnalysis.Absent_HourSk','AbsentHour','column'
---Misconduct Analysis
--EmpID,StoreID,Misconduct_date,Misconduct_ID,Decision_id

Use tescaStaging

select * from Hr.MisconductAnalysis

IF OBJECT_ID('HR.MisconductAnalysis') is not null
Truncate table HR.MisconductAnalysis
--Loading N-1 data,use the query below

use tescaEDW
select count(*) from EDW.fact_MisconductAnalysis

Create table HR.MisconductAnalysis
(
	Misconductpk int identity(1,1),
	EmpID int,
	StoreID int,
	Misconduct_date date,
	Misconduct_id int,
	Decision_id int,
	LoadDate datetime default getdate(),
	constraint Hr_MisconductAnalysis_PK primary key(Misconductpk)

)

select	EmpID,StoreID,Misconduct_date,Misconduct_id,Decision_id	from hr.MisconductAnalysis
Where Misconductpk in
(	
	select max (misconductpk) from hr.MisconductAnalysis
	group by EmpID,StoreID,Misconduct_date,Misconduct_id
)

--Loading TescaEDW
Use tescaEDW

select count(*) as PreCount from edw.fact_misconductAnalysis
select count(*) as PostCount from edw.fact_misconductAnalysis

truncate table EDW.fact_MisconductAnalysis

create table EDW.fact_MisconductAnalysis
(
	MisconductAnalysisSK int identity (1,1),
	EmployeeSK int,
	StoreSK int,
	MisconductDateSK int,
	MisconductSK int,
	DecisionSK int,
	LoadDate datetime ,
	constraint FACT_MisconductAnalysis_SK primary key(MisconductAnalysisSK),
	constraint FACT_MisconductAnalysis_Employeesk foreign key(EmployeeSK ) references edw.dimEmployee(EmployeeSK ),
	constraint FACT_MisconductAnalysis_Storesk foreign key(StoreSK ) references edw.dimstore(StoreSK ),
	constraint FACT_MisconductAnalysis_MisconductDateSK  foreign key(MisconductDateSK  ) references edw.dimdate(datekey ),
	constraint FACT_MisconductAnalysis_MisconductSK  foreign key(MisconductSK  ) references edw.dimMisconduct(MisconductSK),
	constraint FACT_MisconductAnalysis_DecisionSK foreign key(DecisionSK  ) references edw.dimdecision(DecisionSK ),
)
use tescaEDW

Truncate Table [EDW].[fact_absentAnalysis]
truncate table [EDW].[fact_MisconductAnalysis]
Truncate table [EDW].[fact_OvertimeAnalysis]
Truncate table [EDW].[Fact_PurchaseAnalysis]
Truncate table [EDW].[Fact_SalesAnalysis]

select c.customername,t.businessdate as deliverydate,t.BusinessQuater,lineamount,quantity from [EDW].[Fact_SalesAnalysis] F
inner join edw.dimdate t on f.deliverydatesk=t.datekey
inner join edw.dimcustomer c on f.customersk=c.customersk
group by c.customername,t.businessdate,t.BusinessQuater,lineamount,quantity 
order by c.customername ,businessdate asc
select  count(CustomerName) appear,customername,Orderdate,TransDate from retail.salestransaction S
inner join retail.customer c on s.CustomerID=c.CustomerID
group by CustomerName ,TransDate,Orderdate


select max(loaddate) from edw.fact_Overtimeanalysis
Truncate table edw.fact_Absentanalysis
select * from edw.fact_Absentanalysis

select count(*) from edw.fact_Overtimeanalysis
where convert(date,loaddate) ='2023-12-12'


		select TransactionID,TransactionNO,convert(date,transdate) Transdate,convert(date,orderdate) Orderdate,convert(date,deliverydate) 
		Deliverydate,convert(date,Shipdate) Shipdate,VendorID,EmployeeId,ProductID,StoreId,
		Quantity,Lineamount,datediff(day,orderdate,deliverydate)+1 as Diffdate ,getdate() as LoadDate from purchasetransaction