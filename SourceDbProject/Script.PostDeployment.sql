/*
Post-Deployment Script Template							
--------------------------------------------------------------------------------------
 This file contains SQL statements that will be appended to the build script.		
 Use SQLCMD syntax to include a file in the post-deployment script.			
 Example:      :r .\myfile.sql								
 Use SQLCMD syntax to reference a variable in the post-deployment script.		
 Example:      :setvar TableName MyTable							
               SELECT * FROM [$(TableName)]					
--------------------------------------------------------------------------------------
*/

    DECLARE @i INT = 1;
    DECLARE @itemCodes TABLE (ItemCode NVARCHAR(10));
    INSERT INTO @itemCodes VALUES (N'ITEM001'), (N'ITEM002'), (N'ITEM003'), (N'ITEM004'), (N'ITEM005'), (N'ITEM006'), (N'ITEM007'), (N'ITEM008'), (N'ITEM009'), (N'ITEM010');

    DECLARE @StartDate DATETIME = '2025-08-18'; -- fixed start date: 18 Aug 2024
    DECLARE @EndDate DATETIME = DATEADD(DAY, 30, @StartDate); -- 30 days from start date

    WHILE @i <= 200
    BEGIN
        -- Calculate SaleDate evenly distributed over 1 month
        DECLARE @SaleDate DATETIME = DATEADD(MINUTE, ((DATEDIFF(MINUTE, @StartDate, @EndDate) / 200) * (@i - 1)), @StartDate);
        DECLARE @Amount DECIMAL(10,2) = 100.00 + @i;
        DECLARE @ItemCode NVARCHAR(10);

        -- Randomly select an ItemCode
        SELECT TOP 1 @ItemCode = ItemCode FROM @itemCodes ORDER BY NEWID();

        INSERT INTO [dbo].Sales (Id, SaleDate, ItemCode, Amount)
        VALUES (@i, @SaleDate, @ItemCode, @Amount);

        SET @i = @i + 1;
    END
