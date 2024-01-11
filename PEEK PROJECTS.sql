---this is an end to end project from restoring backup file, creating database and designing the schema
---modelling data using the Ralph kimball approach(star schema) of denomalization.

create database PeekOltp
create database PeekStaging
create database PeekEdw
;
use PeekStaging
create schema Retail
;
use PeekEdw
create Schema EDW

---there are two business process in the database;SalesTransaction and PurchaseTransaction
use PeekOltp
 ---studying the dataset to understand best way to solve the business needs
select * from SalesTransaction
select * from PurchaseTransaction
---from studing the dataset:Date is a role playing dimension,employee,product,store are conformed dimensions
--- line amount,tax amount and lineDiscounts Amount are Metrics

---Using the Denomalization modeling technique to design the EDW
Use PeekStaging
	IF OBJECT_ID('PeekStaging..Retail.stgStore') is not null
		DROP table Retail.stgStore
				create table Retail.stgStore
					(
						StoreID int,
						StoreName nvarchar (50),
						StreetAddress nvarchar (50),
						City nvarchar (50),
						State nvarchar (50),
						LoadDate  datetime default getdate(),
						constraint Retail_stgStore_PK Primary Key(StoreID)
					);
			---Loading the denormalized data into store staging
USE PeekOltp
				--Staging Environment Count

							select Count(*) as StgSourceCount from Dbo.Store S
										inner join City C on S.CityID = C.CityID
										Inner join State St on C.StateID =S.StateID
						;
				---Load the ETL Pipelinestaging with the script below
								SELECT StoreID,StoreName,StreetAddress,CityName as City ,State ,getdate() LoadDate from Dbo.Store S
										inner join City C on S.CityID = C.CityID
										Inner join State St on C.StateID =S.StateID
						;
										
--- Loading the Dimension 
USE PeekEdw
						CREATE TABLE EDW.DimStore
							(
								StoreSk int identity (1,1),
								StoreID int,
								StoreName nvarchar (50),
								StreetAddress nvarchar (50),
								City nvarchar(50),
								State nvarchar (50),
								EffectiveStartDate datetime,
								Constraint EDW_DimStore_PK primary key (StoreSk)
							)
						;
				--- Store Dimension PreCount Script
USE PeekEdw
						Select COUNT(*) as PreCount from EDW.DimStore
					;
				---Load the ETL Pipeline Store Dimension with the script below
USE PeekStaging	
							SELECT StoreID,StoreName,StreetAddress,City ,State FROM Retail.stgStore
					;
				--- Store Dimension PostCount After Loading the Dimension
USE PeekEdw
							Select COUNT(*) as PreCount from EDW.DimStore
						;

Use PeekStaging
			IF OBJECT_ID('PeekStaging..Retail.stgEmployee') is not null
				Truncate table Retail.Employee
							Create Table Retail.stgEmployee
								(
									EmployeeID Int,
									EmployeeNo nvarchar (50),
									EmployeeName Nvarchar (200),
									DOB date,
									MaritalStatus nvarchar (50),
									LoadDate datetime default getdate(),
									constraint Retail_Employee_PK Primary Key (EmployeeID)

								)
						;
						--- Employee Staging Source Count Script
USE PeekOltp
						SELECT COUNT(*) SourceCount FROM Employee E
								INNER JOIN MaritalStatus M on E.MaritalStatus = M.MaritalStatusID
								;
						---Loading the Employee Staging Environment Script
								SELECT EmployeeID,EmployeeNo, concat_ws(' ',FirstName,LastName) as EmployeeName,DoB,M.MaritalStatus FROM Employee E
										INNER JOIN MaritalStatus M on E.MaritalStatus = M.MaritalStatusID
						;

---Loading the Employee Dimension 
USE PeekEdw
						CREATE TABLE EDW.DimEmployee
							(
								EmployeeSk int identity(1,1),
								EMployeeID int,
								EmployeeNo nvarchar (50),
								EmployeeName nvarchar (200),
								DOB date,
								MaritalStatus nvarchar (50),
								EffectiveStartDate datetime,
								EffectiveEndDate datetime,
								Constraint EDW_DimEmployee_PK Primary key (EmployeeSk)
							)
					;
					-- Load Employee Dimension 
USE PeekEdw		
					--- Source Count
								SELECT COUNT(*) as Precount FROM Edw.DimEmployee
					;
USE PeekStaging
							   SELECT EmployeeID,EmployeeNo, EmployeeName,DoB,MaritalStatus FROM Retail.stgEmployee
					;
					--- Post Count 
USE PeekEdw
								SELECT COUNT(*) as Postcount FROM Edw.DimEmployee
					;

USE PeekStaging
		IF OBJECT_ID ('PeekStaging..Retail.stgCustomer') is not null
				Truncate Table Retail.stgCustomer
						CREATE TABLE Retail.stgCustomer
							(
								CustomerID int,
								CustomerName nvarchar (100),
								CustomerAddress nvarchar(100),
								City nvarchar (50),
								State nvarchar (50),
								LoadDate datetime default getdate(),
								Constraint Retail_Customer_PK Primary Key(CustomerID)

							)

						--Staging Source Count
Use PeekOltp
						SELECT Count(*) as StgSourceCount FROM Customer C
								INNER JOIN City Ct on C.CityID=Ct.CityID
								INNER JOIN State S on Ct.StateID=S.StateID
					 ;
						--lOADING Script for Customer Staging environment

							SELECT CustomerID, CONCAT(' ',FirstName,LastName) as CustomerName,CustomerAddress,Ct.CityName as City,State,GETDATE() LoadDate FROM Customer C
								INNER JOIN City Ct on C.CityID=Ct.CityID
								INNER JOIN State S on Ct.StateID=S.StateID
						;

---Loading Customer Dimension
Use PeekEdw
						CREATE TABLE EDW.DimCustomer
							(
								CustomerSk int Identity(1,1),
								CustomerID int,
								CustomerName Nvarchar (100),
								CustomerAddress Nvarchar(100),
								City nvarchar (50),
								State nvarchar (50),
								EffectiveStartDate Datetime,
								Constraint EDW_DimCustomer_Pk Primary Key (CustomerSk)
							)
							;

USE PeekEdw 
						--- PreCount DimCustomer
						SELECT COUNT(*) as PreCount FROM EDW.DimCustomer
					;
						--- PostCount 
						SELECT COUNT(*) as PreCount FROM EDW.DimCustomer
					;

						---Loading Script For DimCustomer
USE PeekStaging
								SELECT CustomerID,CustomerName,CustomerAddress,City,State FROM Retail.stgCustomer
						;

USE PeekStaging
		IF OBJECT_ID('PeekStaging..Retail.stgProduct') is not null
			Truncate Table Retail.stgProduct
						CREATE TABLE Retail.stgProduct
							(
								ProductID Int,
								Product nvarchar (50),
								Department nvarchar (50),
								ProductNumber nvarchar(50),
								UnitPrice Float,
								LoadDate Datetime default getdate(),
								Constraint Retail_Product_Pk Primary Key(ProductID)
							)
					;
USe PeekOltp
						---Source Count
						SELECT Count(*) as StgSOurceCount FROM Product P
							INNER JOIN Department D on P.DepartmentID=D.DepartmentID
				;
						---Product Loading Script Staging Enviroment
							SELECT ProductID,Product,Department,ProductNumber,UnitPrice ,getdate()LoadDate FROM Product P
								INNER JOIN Department D on P.DepartmentID=D.DepartmentID
					;

---Loading Product Dimension
USE PeekEdw
					CREATE TABLE EDW.DimProduct
						(
							ProductSK int Identity (1,1),
							ProductID int,
							Product nvarchar (50),
							Department nvarchar (50),
							ProductNumber nvarchar (50),
							Unitprice Float,
							EffectiveStartDate datetime,
							EffectiveEndDate datetime,
							Constraint EDW_DimProduct_pk Primary Key(ProductSk)

						)
					;
					---- Loading Product Dimension Script
USE PeekStaging
					SELECT ProductID,Product,Department,ProductNumber,UnitPrice  FROM Retail.stgProduct 
				;
USE PeekEdw
						---PreCount DimProduct Script
						Select Count(*) from EDW.DimProduct
					;
						--- PostCount DimProduct Script
						Select Count(*) from EDW.DimProduct
					;


Use PeekStaging
		IF OBJECT_ID('PeekStaging..Retail.stgPOSChanel') is not null
		Truncate Table Retail.stgPOSChannel
				Create table Retail.stgPOSChannel
						(
							ChannelID int,
							ChannelNo nvarchar (20),
							DeviceModel nvarchar (50),
							SerailNo nvarchar (50),
							InstallationDate date,
							LoadDate datetime default getdate(),
							Constraint Reatil_stgPOSChannel_PK Primary Key (ChannelID)

						)
					;
					exec sp_rename 'retail.stgPOSChannel.SerailNo','SerialNo','Column'

Use PeekOltp
					---Source Count
					Select Count(*) as StgSourceCount From POSChannel
				;
					---Loading POSChannel Staging Environment
						Select ChannelID,ChannelNo,DeviceModel,SerialNo,InstallationDate,GETDATE() as LoadDate from POSChannel
				;

---Loading POSChannel Dimension Script
USE PeekEdw
					CREATE TABLE EDW.DimPOSChannel
					   (
							ChannelSk int identity (1,1),
							ChannelID int,
							ChannelNo nvarchar (20),
							DeviceModel nvarchar (50),
							SerialNo nvarchar (50),
							EffectiveStartDate datetime,
							EffectiveEndDate datetime,
							Constraint EDW_DimPOSChannel_PK primary key (ChannelSk)

							
						)
				;

					--Loading DimPOSChannel Script
Use PeekStaging
						Select ChannelID,ChannelNo,DeviceModel,SerialNo from Retail.stgPOSChannel

Use PeekEdw
					--PreCount Script
							Select Count(*) as PreCount From EDW.DimPOSChannel
				;
					--PostCount Script
							Select Count(*) as PostCount From EDW.DimPOSChannel
				;


		------Loading Promotion Staging environment Script  with tht Denomalized script
USE PeekOltp
							SELECT PromotionID,Promotion,DiscountPercent,StartDate as PromotionStartDate,EndDate as PromotionEndDate,getdate() LoadDate from Promotion P
							INNER JOIN PromotionType PT on P.PromotionTypeID=PT.PromotionTypeID
					;

						   Select Count(*) stgSourceCount from Promotion P
						   INNER JOIN PromotionType PT on P.PromotionTypeID=PT.PromotionTypeID

	;
USE PeekStaging
		IF OBJECT_ID('PeekStaging..Retail.Promotion') is not null
				Truncate Table Retail.Promotion
							CREATE TABLE Retail.stgPromotion
							(
							PromotionID int,
							Promotion nvarchar (50),
							PromotionStartDate Date,
							PromotionEndDate Date,
							DiscountPercent float,
							LoadDate datetime default getdate(),
							Constraint Retail_stgPromotion_PK Primary key (PromotionID)
							)
					;


			----Loading EDW.DimPromotionn Scipt for ETL Pipeline

USE PeekStaging
							SELECT PromotionID,Promotion,DiscountPercent,PromotionStartDate,PromotionEndDate From Retail.stgPromotion
					;
			

USE PeekEdw
								CREATE TABLE EDW.DimPromotion
								(
								PromotionSk int identity(1,1),
								PromotionID int,
								Promotion nvarchar (50),
								DiscountPercent float,
								PromotionStartDate Date,
								PromotionEndDate Date,
								EffectiveStartDate Datetime,
								Constraint EDW_DimPromotion_Pk Primary key(PromotionSk)
								)
						;
						

								--- Count of EDW
								Select Count(*) as PreCount from edw.DimPromotion
								Select Count(*) as PostCount from edw.DimPromotion

				;

---Loading Vendor Staging and Vendor EDW Script
								--Vendor Loading Script
USE PeekOltp
								SELECT VendorID,VendorNo,Concat(' ',lastname,FirstName)as Name,RegistrationNo,
										VendorAddress as Address,CityName as City,State,getdate() LoadDate FROM Vendor V
								INNER JOIN City C on V.CityID=C.CityID
								INNER JOIN State S  on C.StateID=S.StateID

						;
									--- Staging environment Source Count for Vendor

									SELECT COUNT(*) as stgSourceCount FROM Vendor V
											INNER JOIN City C on V.CityID=C.CityID
											INNER JOIN State S  on C.StateID=S.StateID



USE PeekStaging
		IF OBJECT_ID('PeekStaging..Retail.stgVendor') is not null
				Truncate table Retail.stgVendor 
								CREATE TABLE Retail.stgVendor
								(
								VendorID int,
								VendorNo nvarchar (50),
								Name nvarchar (50),
								RegistrationNo nvarchar (50),
								Address nvarchar (50),
								City nvarchar (50),
								State nvarchar (50),
								LoadDate datetime default getdate(),
								Constraint Retail_stgVendor_PK Primary Key (VendorID)

								)
						;

						---Loading EDW Vendor Script
USE PeekStaging
								SELECT VendorID,VendorNo, Name,RegistrationNo,
										Address,City,State FROM Retail.stgVendor
								
USE PeekEdw
							CREATE TABLE EDW.DimVendor
							(
							VendorSK int identity(1,1),
							VendorID int,
							VendorNo nvarchar (50),
							Name nvarchar (50),
							RegistrationNo nvarchar (50),
							Address nvarchar (50),
							City nvarchar (50),
							State nvarchar (50),
							EffectiveStartDate datetime,
							EffectiveEndDate datetime,
							Constraint EDW_DimVendor_PK Primary Key(VendorSk)

							)


				;

									---EDW PreCounts Before Loading
									SELECT COUNT(*) as PreCount From EDW.DimVendor;

									--Edw PostCount after Loading EDW.DimVendor
									SELECT COUNT(*) as PreCount From EDW.DimVendor

						;


USE PeekEdw
							CREATE TABLE EDW.DimDate
							(
							DateSK int ,
							BusinessDate Date,
							BusinessMonth int,
							BusinessYear int,
							BusinessDay int,
							BusinessDayOfWeek int,
							BusinessQuater nvarchar(50),
							EnglishMonth nvarchar(50),
							EnglishDay nvarchar(50),
							FrenchMonth nvarchar(50),
							FrenchDay nvarchar(50),
							EffectiveStartDate datetime
							Constraint EDW_DimDate_pk Primary Key (DateSk)
							)
						;

USE PeekOltp

								---GENEARATE A DATE TABLE TO HANDLE THE DIFFERENT ROLES OF DATE AS WELL AS GENERATE FURTURISTIC
								--DATES THAT CAN SUPPORT TRENDS AND PREDICTIVE ANALYSIS
								--- USED STORE PROCEDURE AND VARIABLE DECLARATION
CREATE PROCEDURE spDATEGENERATOR( @YEARS INT)
			AS
BEGIN
		SET NOCOUNT ON
			IF OBJECT_ID('PeekEDW..EDW.DimDate') IS NOT NULL
					TRUNCATE TABLE EDW.DimDate

						DECLARE @STARTDATE DATE =
									(
											SELECT MIN(CONVERT(DATE,TRANSDATE)) FROM PEEKOLTP.DBO.SALESTRANSACTION
														UNION
											SELECT MIN(CONVERT(DATE,TRANSDATE)) FROM PEEKOLTP.DBO.PurchaseTransaction

										)
						DECLARE @ENDDATE DATE 
						
						DECLARE @DAYSDIFF INT
						dECLARE @CURRENTDATE dATE
						DECLARE @CURRENTDAY INT =0

									---THIS CAPUTURES THE PREVIOUS YEAR
								SELECT @STARTDATE=DATEADD(YEAR,-1,@STARTDATE)
													--CREATING A COMPLETE CALENDER YEAR FOR FUTURE DATE HENCE THE THE DATEFROMPARTS
								SELECT @ENDDATE=DATEADD(DD,@YEARS,DATEFROMPARTS(YEAR(GETDATE()),12,31))
								SELECT @DAYSDIFF = DATEDIFF(DD,@STARTDATE,@ENDDATE)

				WHILE 
						@CURRENTDAY<=@DAYSDIFF

			BEGIN
								SELECT @CURRENTDATE=DATEADD(DD,@CURRENTDAY,@STARTDATE)

											INSERT INTO EDW.DimDate
											(
														DateSK,BusinessDate,BusinessMonth ,BusinessYear ,BusinessDay ,BusinessDayOfWeek ,
													BusinessQuater,EnglishMonth ,EnglishDay ,FrenchMonth ,FrenchDay ,EffectiveStartDate
											)

											SELECT CONVERT(INT,CONVERT(NVARCHAR (8),@CURRENTDATE,112)) AS DateSk, @CURRENTDATE as BusinessDate,
													Month(@currentDate) BusinessMonth,Year(@CurrentDate) BusinessYear,
														day(@Currentdate) BusinessDay,DATEPART(dw,@CurrentDate) BusinessDayOfWeek,
														Concat('Q',DATEPART(QUARTER,@CURRENTDATE)) BusinessQuater,
														DATENAME(month,@Currentdate) EnglishMonth,DATENAME(dw,@CURRENTDATE) EnglishDay,


						case
									datepart(month,@currentdate)
										when 1 then 'Janvier' when 2 then 'fevrier' when 3 then 'Mars' when 4 then 'Avril'
										when 5 then 'Mai' when 6 then 'Juin' when 7 then 'Juillet' when 8 then 'aout'
										when 9 then'Septembre' when 10 then 'Octobre' when 11 then 'Novembre' when 12 then 'Decembre'
										end as 'FrenchMonth',

						case 
										datepart(Weekday,@currentdate)
										when 1 then 'Dimanche' when 2 then 'lundi' when 3 then 'Mardi' when 4 then 'Mercredi'
										when 5 then 'Jeudi' when 6 then 'Vendredi' when 7 then 'Samedi' 
										End as FrenchDay,

										getdate() EffectiveStartdate
		
				SELECT @CURRENTDAY=@CURRENTDAY + 1

		END
END
;

EXEC spDATEGENERATOR 10


----TIME DIMENSION
USE PeekEdw


CREATE TABLE EDW.DimTime
					(
					TimeSK int identity (1,1),
					HourofDAY INT,
					PeriodsofDay nvarchar(50),
					BusinessHours nvarchar(50),
					OnlineHours nvarchar (50),
					EffectiveStartDate datetime
Constraint EDW_Dimtime_PK primary key (TimeSk)
)
;

CREATE PROCEDURE EDW.spTimeGenerator
		AS
BEGIN
		SET NOCOUNT ON
			IF OBJECT_ID('PeekEDW.EDW.DimTime') is not null
				TRUNCATE TABLE EDW.DimTime
---12 Am to 12:59 ===>0,1am to 11:59pm =23hours
				DECLARE @StartHour int=0
				DECLARE @EndHour int=23
				WHILE @StartHour<=@EndHour
				BEGIN
				INSERT INTO EDW.DimTime
								(
								HourofDAY,PeriodsofDay,BusinessHours,onlineHours,EffectiveStartDate

								)

							SELECT @StartHour as HourofDAY,
						CASE
								WHEN @StartHour=0 THEN 'Midnight'
								WHEN @StartHour>=1 AND @StartHour <=4 THEN 'Early Morning'
								WHEN @StartHour>=5 AND @StartHour <=11 THEN 'Morning'
								WHEN @StartHour=12 THEN 'Noon'
								WHEN @StartHour>=13 AND @StartHour <=17 THEN 'AfterNoon'
								WHEN @StartHour>=18 AND @StartHour <=20 THEN 'Evening'
								WHEN @StartHour>=21 AND @StartHour <=23 THEN 'Night'
						END AS PeriodsofDay,

						CASE
		
								WHEN @StartHour>=0 AND @StartHour <=8 THEN 'Closed'
								WHEN @StartHour>=9 AND @StartHour <=18 THEN 'Open'
								WHEN @StartHour>=19 AND @StartHour <=23 THEN 'Closed'

						END AS BusinessHours,
					CASE
		
								WHEN @StartHour>=0 AND @StartHour <=23 THEN 'Open'
				
						END AS OnlineHours,

						GETDATE() as EffectiveStartDate
		SELECT @StartHour=@StartHour +1


		END
END

EXEC EDW.spTimeGenerator


---LOADING SALESTRANSACTION TABLE INTO THE STAGING


SELECT TransactionID,TransactionNO,TransDate,OrderDate,DeliveryDate,ChannelID,CustomerID,EmployeeID
,ProductID,StoreID,PromotionID,Quantity,TaxAmount,LineAmount,LineDiscountAmount FROM PeekOLTP.Dbo.SalesTransaction
Use PeekStaging
						CREATE TABLE Retail.stgSalesAnalysis
					(
							TransactionID INT,
							TransactionNO nvarchar(50),
							TransDate date,
							OrderDate date,
							DeliveryDate date,
							OrderHour int,
							TransHour int,
							ChannelID int,
							CustomerID int,
							EmployeeID int,
							ProductID int,
							StoreID int,
							PromotionID int,
							Quantity int,
							TaxAmount int,
							LineAmount int,
							LineDiscountAmount float,
							LoadDate datetime default getdate(),
							Constraint Retail_stgSalesAnanlysis_pk primary key (TransactionID)
					)


					;
---Loading SALESANLYSIS STAGING IS DEPENDENT ON THE NUMBER OF DATA IN THE EDW. THE COUNT IS VERY VITAL TO SUCCESS

IF 
				(SELECT COUNT(*) FROM PeekEdw.EDW.Fact_SalesAnalysis) =0

							SELECT TransactionID,TransactionNO,convert(date,TransDate) TransDate ,convert(date,OrderDate) OrderDate,
							convert(date,DeliveryDate) DeliveryDate,datepart(hour ,OrderDate) OrderHour,datepart(hour,Transdate) transHour,
							ChannelID,CustomerID,EmployeeID,ProductID,StoreID,PromotionID
						,Quantity,TaxAmount,LineAmount,LineDiscountAmount FROM PeekOltp.dbo.SalesTransaction 

					WHERE convert(date,TransDate) <= convert(date,DATEADD(DD,-1,getdate()))

Else

				SELECT TransactionID,TransactionNO,convert(date,TransDate) TransDate ,convert(date,OrderDate) OrderDate,
							convert(date,DeliveryDate) DeliveryDate,datepart(hour ,OrderDate) OrderHour,datepart(hour,Transdate) transHour,
							ChannelID,CustomerID,EmployeeID,ProductID,StoreID,PromotionID
						,Quantity,TaxAmount,LineAmount,LineDiscountAmount FROM PeekOltp.dbo.SalesTransaction 

					WHERE convert(date,TransDate) = convert(date,DATEADD(DD,-1,getdate()))


			--STAGING SOURCE COUNT
IF
		(SELECT COUNT(*) FROM PeekEdw.EDW.Fact_SalesAnalysis) =0
			
				SELECT COUNT(*)As stgSourceCount FROM PeekOltp.dbo.SalesTransaction
				WHERE CONVERT(date,TransDate) <=CONVERT(DATE,DATEADD(DD,-1,GETDATE()))

ELSE
		SELECT COUNT(*)AS stgSourceCount FROM PeekOltp.dbo.SalesTransaction
				WHERE CONVERT(date,TransDate) =CONVERT(DATE,DATEADD(DD,-1,GETDATE())) 
				
;

USE PeekEdw

			--Loading the Fact Table

CREATE TABLE EDW.Fact_SalesAnalysis
(
SalesAnalysisSk INT,
TransactionNO nvarchar(50),
TransDateSK int,
OrderDateSK int,
DeliveryDateSK int,
OrderHourSK int,
TransHourSk int,
ChannelSK int,
CustomerSK int,
EmployeeSK int,
ProductSK int,
StoreSK int,
PromotionSK int,
Quantity int,
TaxAmount int,
LineAmount int,
LineDiscountAmount float,
LoadDate datetime default getdate(),

Constraint EDW_Fact_SalesAnalysis_Pk Primary key (SalesAnalysisSk),
Constraint EDW_Fact_SalesAnalysis_TransDateSK_FK foreign key(TransDateSK) references EDW.DimDate(DateSK),
Constraint EDW_Fact_SalesAnalysis_OrderDateSk_Fk Foreign key (OrderDateSK) references EDW.DimDate(DateSK),
Constraint EDW_Fact_SalesAnalysis_DeliveryDateSk_Fk foreign key (DeliverydateSK) references EDW.DimDate(DateSk),
Constraint EDW_Fact_SalesAnalysis_OrderHourSK_FK Foreign Key (OrderHourSK) references EDW.DimTime(TimeSk),
Constraint EDW_Fact_SalesAnalysis_TransHourSk_FK Foreign key (TransHourSk) references EDW.DimTime(Timesk),
Constraint EDW_Fact_SalesAnalysis_ChannelSk_FK Foreign Key (ChannelSk) references EDW.DimPOSChannel(channelsk),
Constraint EDW_Fact_SalesAnalysis_CustomerSk_FK Foreign key (CustomerSk) References EDW.DimCustomer(Customersk),
Constraint EDW_Fact_SalesAnalysis_EmployeeSk_FK Foreign Key (EmployeeSk) References EDW.DimEmployee(EmployeeSK),
Constraint EDW_Fact_SalesAnalysis_productSK_FK Foreign key (ProductSK) references EDW.DimProduct(ProductSK),
Constraint EDW_Fact_SalesAnalysis_StoreSk_FK foreign key (StoreSK) references EDW.DimStore(StoreSK),
Constraint EDW_Fact_SalesAnalysis_PromotionSK foreign key(Promotionsk) references EDW.DimPromotion(promotionsk)


)

--- loading EDW FACT ANALYSIS TABLE WITH THE SCRIPT BELOW.
SELECT TransactionNO,TransDate,OrderDate,DeliveryDate,OrderHour,TransHour,ChannelID,CustomerID,EmployeeID
,ProductID,StoreID,PromotionID,Quantity,TaxAmount,LineAmount,LineDiscountAmount,GETDATE() LoadDate FROM PeekStaging.Retail.stgSalesAnalysis

---EDW COUNTS
		SELECT COUNT(*) PreCount FROM EDW.Fact_SalesAnalysis 
		SELECT COUNT(*) PostCOunt FROM EDW.Fact_SalesAnalysis 

