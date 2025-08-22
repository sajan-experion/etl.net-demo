
CREATE TABLE [dbo].[SalesAggregation] (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    AggregationDate DATE NOT NULL,
    AggregationType NVARCHAR(10) NOT NULL, -- DAILY, WEEKLY, MONTHLY
    ItemCode NVARCHAR(50) NOT NULL,
    TotalAmount DECIMAL(18,2) NOT NULL,
    TransactionCount INT NOT NULL,
    AverageAmount DECIMAL(18,4) NOT NULL,
    ProcessedAt DATETIME2 NOT NULL,
    PeriodKey NVARCHAR(20) NOT NULL, -- e.g., "2025-08-19", "2025-W33", "2025-08"
    UNIQUE(AggregationType, PeriodKey, ItemCode)
);

GO

-- Useful indexes for querying
CREATE INDEX IX_SalesAggregation_AggregationType_Date ON [dbo].[SalesAggregation] (AggregationType, AggregationDate);
GO

CREATE INDEX IX_SalesAggregation_ItemCode ON [dbo].[SalesAggregation] (ItemCode);
GO

CREATE INDEX IX_SalesAggregation_PeriodKey ON [dbo].[SalesAggregation] (PeriodKey);
GO

