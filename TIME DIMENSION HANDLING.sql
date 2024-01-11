/*
Description: This script creates a time dimension table (EDW.DimTime) and a stored procedure (EDW.spTimeGenerator) to generate time-related data.

Table: EDW.DimTime
- TimeSK: Unique identifier for each record.
- HourofDAY: Hour of the day.
- PeriodsofDay: Categorization of hours into different periods (e.g., Morning, Afternoon).
- BusinessHours: Classification of business hours as Open or Closed.
- OnlineHours: Classification of online hours as Open.
- EffectiveStartDate: Timestamp indicating when the record was created.

Stored Procedure: EDW.spTimeGenerator
- Generates time-related data and populates the EDW.DimTime table.
*/

CREATE TABLE EDW.DimTime
					(
					TimeSK int identity (1,1),
					HourofDAY INT,
					PeriodsofDay nvarchar(50),
					BusinessHours nvarchar(50),
					OnlineHours nvarchar (50),
					EffectiveStartDate datetime

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
SELECT * FROM EDW.DimTime