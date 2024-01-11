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
