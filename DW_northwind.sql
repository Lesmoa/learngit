---------------------------------------
--Data Warehousing Assignment 2 - Phase 2
--My Northwind's DW ETL+DQ Script File
--Student ID:   	1462832 / GuanB04
--Student Name: 	Bingfei Guan
------------------------------------

print '***************************************************************'
print '****** Section 1: Creating DW Tables'
print '***************************************************************'

print 'Drop all DW tables (except dimTime)'
--Add drop statements below...
--DO NOT DROP dimTime table as you must have used Script provided on the Moodle to create it
DROP TABLE factOrders;
DROP TABLE dimCustomers;
DROP TABLE dimProducts;
DROP TABLE dimSuppliers;
DROP TABLE BufferOrdersNW7;
DROP TABLE BufferOrdersNW8;
print 'Creating all dimension tables required'
--Add statements below... 
BEGIN
CREATE TABLE [dbo].[dimCustomers](
	[CustomerKey] [int] IDENTITY(1,1) NOT NULL,
	[CustomerID] [nchar](5) NOT NULL,
	[CompanyName] [nvarchar](40) NOT NULL,
	[ContactName] [nvarchar](30) NULL,
	[ContactTitle] [nvarchar](30) NULL,
	[Address] [nvarchar](60) NULL,
	[City] [nvarchar](15) NULL,
	[Region] [nvarchar](15) NULL,
	[PostalCode] [nvarchar](10) NULL,
	[Country] [nvarchar](15) NULL,
	[Phone] [nvarchar](24) NULL,
	[Fax] [nvarchar](24) NULL,
	CONSTRAINT PK_DIMCUSTOMERS_CUSTOMERKEY PRIMARY KEY ([CustomerKey])
) ON [PRIMARY]
END
GO

BEGIN
CREATE TABLE [dbo].[dimProducts](
	[ProductKey] [int] IDENTITY(1,1) NOT NULL,
	[ProductID] [int] NOT NULL,
	[ProductName] [nvarchar](40) NOT NULL,
	[QuantityPerUnit] [nvarchar](20) NULL,
	[UnitPrice] [money] NULL,
	[UnitsInStock] [smallint] NULL,
	[UnitsOnOrder] [smallint] NULL,
	[ReorderLevel] [smallint] NULL,
	[Discontinued] [bit] NOT NULL,
	[CategoryName] [nvarchar](15) NOT NULL,
	[Description] [ntext] NULL,
	[Picture] [image] NULL,
	CONSTRAINT PK_DIMPRODUCTS_PRODUCTKEY PRIMARY KEY ([ProductKey])
) ON [PRIMARY]
END
GO

BEGIN
CREATE TABLE [dbo].[dimSuppliers](
	[SupplierKey] [int] IDENTITY(1,1) NOT NULL,
	[SupplierID] [int] NOT NULL,
	[CompanyName] [nvarchar](40) NOT NULL,
	[ContactName] [nvarchar](30) NULL,
	[ContactTitle] [nvarchar](30) NULL,
	[Address] [nvarchar](60) NULL,
	[City] [nvarchar](15) NULL,
	[Region] [nvarchar](15) NULL,
	[PostalCode] [nvarchar](10) NULL,
	[Country] [nvarchar](15) NULL,
	[Phone] [nvarchar](24) NULL,
	[Fax] [nvarchar](24) NULL,
	[Homepage] [ntext] NULL,
	CONSTRAINT PK_DIMSUPPLIERS_PRODUCTKEY PRIMARY KEY ([SupplierKey])
) ON [PRIMARY]
END
GO

BEGIN
CREATE TABLE [dbo].[BufferOrdersNW7](
	[OrderID] [int] NOT NULL,
	[CustomerID] [nchar](5) NULL,
	[EmployeeID] [int] NULL,
	[OrderDate] [datetime] NULL,
	[RequiredDate] [datetime] NULL,
	[ShippedDate] [datetime] NULL,
	[ShipVia] [int] NULL,
	[Freight] [money] NULL,
	[ShipName] [nvarchar](40) NULL,
	[ShipAddress] [nvarchar](60) NULL,
	[ShipCity] [nvarchar](15) NULL,
	[ShipRegion] [nvarchar](15) NULL,
	[ShipPostalCode] [nvarchar](10) NULL,
	[ShipCountry] [nvarchar](15) NULL
) ON [PRIMARY]
END
GO

BEGIN
CREATE TABLE [dbo].[BufferOrdersNW8](
	[OrderID] [int] NOT NULL,
	[CustomerID] [nchar](5) NULL,
	[EmployeeID] [int] NULL,
	[OrderDate] [datetime] NULL,
	[RequiredDate] [datetime] NULL,
	[ShippedDate] [datetime] NULL,
	[ShipVia] [int] NULL,
	[Freight] [money] NULL,
	[ShipName] [nvarchar](40) NULL,
	[ShipAddress] [nvarchar](60) NULL,
	[ShipCity] [nvarchar](15) NULL,
	[ShipRegion] [nvarchar](15) NULL,
	[ShipPostalCode] [nvarchar](10) NULL,
	[ShipCountry] [nvarchar](15) NULL
) ON [PRIMARY]
END
GO

print 'Creating Staging Area table(BufferOrders)'
INSERT INTO [dbo].[BufferOrdersNW7]
           ([OrderID]
           ,[CustomerID]
           ,[EmployeeID]
           ,[OrderDate]
           ,[RequiredDate]
           ,[ShippedDate]
           ,[ShipVia]
           ,[Freight]
           ,[ShipName]
           ,[ShipAddress]
           ,[ShipCity]
           ,[ShipRegion]
           ,[ShipPostalCode]
           ,[ShipCountry])
	    SELECT 
           OrderID,
           CustomerID,
           EmployeeID,
           OrderDate,
           RequiredDate,
           ISNULL(ShippedDate, '9999-12-31'),
           ShipVia,
           Freight,
           ShipName,
           ShipAddress,
           ShipCity,
           ShipRegion,
           ShipPostalCode,
           ShipCountry
		FROM northwind7.dbo.Orders
GO

INSERT INTO [dbo].[BufferOrdersNW8]
           ([OrderID]
           ,[CustomerID]
           ,[EmployeeID]
           ,[OrderDate]
           ,[RequiredDate]
           ,[ShippedDate]
           ,[ShipVia]
           ,[Freight]
           ,[ShipName]
           ,[ShipAddress]
           ,[ShipCity]
           ,[ShipRegion]
           ,[ShipPostalCode]
           ,[ShipCountry])
	    SELECT 
           OrderID,
           CustomerID,
           EmployeeID,
           OrderDate,
           RequiredDate,
           ISNULL(ShippedDate, '9999-12-31'),
           ShipVia,
           Freight,
           ShipName,
           ShipAddress,
           ShipCity,
           ShipRegion,
           ShipPostalCode,
           ShipCountry
		FROM northwind8.dbo.Orders
GO

print 'Creating a fact table required'
--Add statements below... 
BEGIN
CREATE TABLE [dbo].[factOrders](
	[ProductKey] [int] NOT NULL,
	[CustomerKey] [int] NOT NULL,
	[SupplierKey] [int] NOT NULL,
	[OrderDateKey] [int] NOT NULL,
	[RequiredDateKey] [int] NOT NULL,
	[ShippedDateKey] [int] NOT NULL,
	[OrderID] [int] NOT NULL,
	[UnitPrice] [money] NOT NULL,
	[Qty] [smallint] NOT NULL,
	[Discount] [real] NOT NULL,
	[TotalPrice] [money] NOT NULL,
	[ShipperCompany] [nvarchar](40) NOT NULL,
	[ShipperPhone] [nvarchar](24) NULL,
	CONSTRAINT PK_FACTORDERS PRIMARY KEY ([ProductKey], [CustomerKey], [SupplierKey], [OrderDateKey]),
	CONSTRAINT FK_DIMPRODUCTS_FACTORDERS_PRODUCTKEY FOREIGN KEY ([ProductKey]) REFERENCES dimProducts([ProductKey]),
	CONSTRAINT FK_DIMCUSTOMERS_FACTORDERS_CUSTOMERKEY FOREIGN KEY ([CustomerKey]) REFERENCES dimCustomers([CustomerKey]),
	CONSTRAINT FK_DIMSUPPLIERS_FACTORDERS_SUPPLIERKEY FOREIGN KEY ([SupplierKey]) REFERENCES dimSuppliers([SupplierKey]),
	CONSTRAINT FK_DIMTIME_FACTORDERS_ORDERDATEKEY FOREIGN KEY ([OrderDateKey]) REFERENCES dimTime([TimeKey]),
	CONSTRAINT FK_DIMTIME_FACTORDERS_REQUIREDDATEKEY FOREIGN KEY ([RequiredDateKey]) REFERENCES dimTime([TimeKey]),
	CONSTRAINT FK_DIMTIME_FACTORDERS_SHIPPEDDATEKEY FOREIGN KEY ([ShippedDateKey]) REFERENCES dimTime([TimeKey])
) ON [PRIMARY]
END

print '***************************************************************'
print '****** Section 2: Populate DW Dimension Tables (except dimTime)'
print '***************************************************************'

print 'Populating all dimension tables from northwind7 and northwind8'
--Add statements below... 
--IMPORTANT! All Data in dimension tables MUST satisfy all the defined DQ Rules 
print '***************************************************************'
print '****** Modify the MERGE statement for dimCustomers.'
print '***************************************************************'
MERGE INTO dimCustomers AS dc
USING (SELECT *
         FROM northwind7.dbo.Customers c
        WHERE c.%%physloc%% NOT IN
              (SELECT RowID
                 FROM DQLog
                WHERE DBName = 'northwind7'
                  AND TableName = 'Customers'
                  AND RuleNo = 4
                  AND Action = 'fix')) c
ON dc.CustomerID = c.CustomerID COLLATE DATABASE_DEFAULT
WHEN NOT MATCHED THEN
  INSERT
  VALUES
    (c.CustomerID,
     c.CompanyName,
     c.ContactName,
     c.ContactTitle,
     c.Address,
     c.City,
     c.Region,
     c.PostalCode,
     c.Country,
     c.Phone,
     c.Fax)
WHEN MATCHED THEN
  UPDATE
     SET dc.CustomerID   = c.CustomerID,
         dc.CompanyName  = c.CompanyName,
         dc.ContactName  = c.ContactName,
         dc.ContactTitle = c.ContactTitle,
         dc.Address      = c.Address,
         dc.City         = c.City,
         dc.Region       = c.Region,
         dc.PostalCode   = c.PostalCode,
         dc.Country      = c.Country,
         dc.Phone        = c.Phone,
         dc.Fax          = c.Fax;
GO

MERGE INTO dimCustomers dc
USING (SELECT *
         FROM northwind7.dbo.Customers c
        WHERE c.%%physloc%% IN
              (SELECT RowID
                 FROM DQLog
                WHERE DBName = 'northwind7'
                  AND TableName = 'Customers'
                  AND RuleNo = 4
                  AND Action = 'fix')) c
ON dc.CustomerID = c.CustomerID COLLATE DATABASE_DEFAULT
WHEN NOT MATCHED THEN
  INSERT
  VALUES
    (c.CustomerID,
     c.CompanyName,
     c.ContactName,
     c.ContactTitle,
     c.Address,
     c.City,
     c.Region,
     c.PostalCode,
     'USA',
     c.Phone,
     c.Fax)
WHEN MATCHED THEN
  UPDATE
     SET dc.CustomerID   = c.CustomerID,
         dc.CompanyName  = c.CompanyName,
         dc.ContactName  = c.ContactName,
         dc.ContactTitle = c.ContactTitle,
         dc.Address      = c.Address,
         dc.City         = c.City,
         dc.Region       = c.Region,
         dc.PostalCode   = c.PostalCode,
         dc.Country      = 'USA',
         dc.Phone        = c.Phone,
         dc.Fax          = c.Fax;
GO

MERGE INTO dimCustomers AS dc
USING (SELECT *
         FROM northwind8.dbo.Customers c
        WHERE c.%%physloc%% NOT IN
              (SELECT RowID
                 FROM DQLog
                WHERE DBName = 'northwind8'
                  AND TableName = 'Customers'
                  AND RuleNo = 4
                  AND Action = 'fix')) c
ON dc.CustomerID = c.CustomerID COLLATE DATABASE_DEFAULT
WHEN NOT MATCHED THEN
  INSERT
  VALUES
    (c.CustomerID,
     c.CompanyName,
     c.ContactName,
     c.ContactTitle,
     c.Address,
     c.City,
     c.Region,
     c.PostalCode,
     c.Country,
     c.Phone,
     c.Fax)
WHEN MATCHED THEN
  UPDATE
     SET dc.CustomerID   = c.CustomerID,
         dc.CompanyName  = c.CompanyName,
         dc.ContactName  = c.ContactName,
         dc.ContactTitle = c.ContactTitle,
         dc.Address      = c.Address,
         dc.City         = c.City,
         dc.Region       = c.Region,
         dc.PostalCode   = c.PostalCode,
         dc.Country      = c.Country,
         dc.Phone        = c.Phone,
         dc.Fax          = c.Fax;
GO

MERGE INTO dimCustomers dc
USING (SELECT *
         FROM northwind8.dbo.Customers c
        WHERE c.%%physloc%% IN
              (SELECT RowID
                 FROM DQLog
                WHERE DBName = 'northwind8'
                  AND TableName = 'Customers'
                  AND RuleNo = 4
                  AND Action = 'fix')) c
ON dc.CustomerID = c.CustomerID COLLATE DATABASE_DEFAULT
WHEN NOT MATCHED THEN
  INSERT
  VALUES
    (c.CustomerID,
     c.CompanyName,
     c.ContactName,
     c.ContactTitle,
     c.Address,
     c.City,
     c.Region,
     c.PostalCode,
     'UK',
     c.Phone,
     c.Fax)
WHEN MATCHED THEN
  UPDATE
     SET dc.CustomerID   = c.CustomerID,
         dc.CompanyName  = c.CompanyName,
         dc.ContactName  = c.ContactName,
         dc.ContactTitle = c.ContactTitle,
         dc.Address      = c.Address,
         dc.City         = c.City,
         dc.Region       = c.Region,
         dc.PostalCode   = c.PostalCode,
         dc.Country      = 'UK',
         dc.Phone        = c.Phone,
         dc.Fax          = c.Fax;
GO

print '***************************************************************'
print '****** Modify the MERGE statement for dimProducts.'
print '***************************************************************'
MERGE INTO dimProducts dp
USING (SELECT p.ProductID,
              p.ProductName,
              p.QuantityPerUnit,
              p.UnitPrice,
              p.UnitsInStock,
              p.UnitsOnOrder,
              p.ReorderLevel,
              p.Discontinued,
              c.CategoryName,
              c.Description,
              c.Picture
         FROM northwind7.dbo.Products p, northwind7.dbo.Categories c
        WHERE p.CategoryID = c.CategoryID
          AND p.%%physloc%% NOT IN
              (SELECT RowID
                 FROM DQLog
                WHERE DBName = 'northwind7'
                  AND TableName = 'products'
                  AND RuleNo IN (1, 6)
                  AND Action = 'reject')) pc
ON (dp.ProductID = pc.ProductID)
WHEN NOT MATCHED THEN
  INSERT
  VALUES
    (pc.ProductID,
     pc.ProductName,
     pc.QuantityPerUnit,
     pc.UnitPrice,
     pc.UnitsInStock,
     pc.UnitsOnOrder,
     pc.ReorderLevel,
     pc.Discontinued,
     pc.CategoryName,
     pc.Description,
     pc.Picture)
WHEN MATCHED THEN
  UPDATE
     SET dp.ProductID       = pc.ProductID,
         dp.ProductName     = pc.ProductName,
         dp.QuantityPerUnit = pc.QuantityPerUnit,
         dp.UnitPrice       = pc.UnitPrice,
         dp.UnitsInStock    = pc.UnitsInStock,
         dp.UnitsOnOrder    = pc.UnitsOnOrder,
         dp.ReorderLevel    = pc.ReorderLevel,
         dp.Discontinued    = pc.Discontinued,
         dp.CategoryName    = pc.CategoryName,
         dp.Description     = pc.Description,
         dp.Picture         = pc.Picture;
GO

MERGE INTO dimProducts dp
USING (SELECT p.ProductID,
              p.ProductName,
              p.QuantityPerUnit,
              p.UnitPrice,
              p.UnitsInStock,
              p.UnitsOnOrder,
              p.ReorderLevel,
              p.Discontinued,
              c.CategoryName,
              c.Description,
              c.Picture
         FROM northwind8.dbo.Products p, northwind8.dbo.Categories c
        WHERE p.CategoryID = c.CategoryID
          AND p.%%physloc%% NOT IN
              (SELECT RowID
                 FROM DQLog
                WHERE DBName = 'northwind8'
                  AND TableName = 'products'
                  AND RuleNo IN (1, 6)
                  AND Action = 'reject')) pc
ON (dp.ProductID = pc.ProductID)
WHEN NOT MATCHED THEN
  INSERT
  VALUES
    (pc.ProductID,
     pc.ProductName,
     pc.QuantityPerUnit,
     pc.UnitPrice,
     pc.UnitsInStock,
     pc.UnitsOnOrder,
     pc.ReorderLevel,
     pc.Discontinued,
     pc.CategoryName,
     pc.Description,
     pc.Picture)
WHEN MATCHED THEN
  UPDATE
     SET dp.ProductID       = pc.ProductID,
         dp.ProductName     = pc.ProductName,
         dp.QuantityPerUnit = pc.QuantityPerUnit,
         dp.UnitPrice       = pc.UnitPrice,
         dp.UnitsInStock    = pc.UnitsInStock,
         dp.UnitsOnOrder    = pc.UnitsOnOrder,
         dp.ReorderLevel    = pc.ReorderLevel,
         dp.Discontinued    = pc.Discontinued,
         dp.CategoryName    = pc.CategoryName,
         dp.Description     = pc.Description,
         dp.Picture         = pc.Picture;
GO

print '***************************************************************'
print '****** Modify the MERGE statement for dimSuppliers.'
print '***************************************************************'
MERGE INTO dimSuppliers AS ds
USING (SELECT *
         FROM northwind7.dbo.Suppliers su
        WHERE su.%%physloc%% NOT IN
              (SELECT RowID
                 FROM DQLog
                WHERE DBName = 'northwind7'
                  AND TableName = 'Suppliers'
                  AND RuleNo = 4
                  AND Action = 'fix')) su
ON ds.SupplierID = su.SupplierID
WHEN NOT MATCHED THEN
  INSERT
  VALUES
    (su.SupplierID,
     su.CompanyName,
     su.ContactName,
     su.ContactTitle,
     su.Address,
     su.City,
     su.Region,
     su.PostalCode,
     su.Country,
     su.Phone,
     su.Fax,
     su.Homepage)
WHEN MATCHED THEN
  UPDATE
     SET ds.SupplierID   = su.SupplierID,
         ds.CompanyName  = su.CompanyName,
         ds.ContactName  = su.ContactName,
         ds.ContactTitle = su.ContactTitle,
         ds.Address      = su.Address,
         ds.City         = su.City,
         ds.Region       = su.Region,
         ds.PostalCode   = su.PostalCode,
         ds.Country      = su.Country,
         ds.Phone        = su.Phone,
         ds.Fax          = su.Fax,
         ds.Homepage     = su.Homepage;
GO

MERGE INTO dimSuppliers AS ds
USING (SELECT su.SupplierID,
              su.CompanyName,
              su.ContactName,
              su.ContactTitle,
              su.Address,
              su.City,
              su.Region,
              su.PostalCode,
			  CASE 
              WHEN lower(su.Country) in ('us', 'united states') THEN 'USA' 
              WHEN lower(su.Country) in ('united kingdom', 'britain') THEN 'UK' 
              END AS CorrectedCountry,
              su.Country,
              su.Phone,
              su.Fax,
              su.Homepage
         FROM northwind7.dbo.Suppliers su
        WHERE su.%%physloc%% IN
              (SELECT RowID
                 FROM DQLog
                WHERE DBName = 'northwind7'
                  AND TableName = 'Suppliers'
                  AND RuleNo = 4
                  AND Action = 'fix')) su
ON ds.SupplierID = su.SupplierID
WHEN NOT MATCHED THEN
  INSERT
  VALUES
    (su.SupplierID,
     su.CompanyName,
     su.ContactName,
     su.ContactTitle,
     su.Address,
     su.City,
     su.Region,
     su.PostalCode,
     CorrectedCountry,
     su.Phone,
     su.Fax,
     su.Homepage)
WHEN MATCHED THEN
  UPDATE
     SET ds.SupplierID   = su.SupplierID,
         ds.CompanyName  = su.CompanyName,
         ds.ContactName  = su.ContactName,
         ds.ContactTitle = su.ContactTitle,
         ds.Address      = su.Address,
         ds.City         = su.City,
         ds.Region       = su.Region,
         ds.PostalCode   = su.PostalCode,
         ds.Country      = CorrectedCountry,
         ds.Phone        = su.Phone,
         ds.Fax          = su.Fax,
         ds.Homepage     = su.Homepage;
GO

MERGE INTO dimSuppliers AS ds
USING (SELECT *
         FROM northwind8.dbo.Suppliers su
        WHERE su.%%physloc%% NOT IN
              (SELECT RowID
                 FROM DQLog
                WHERE DBName = 'northwind8'
                  AND TableName = 'Suppliers'
                  AND RuleNo = 4
                  AND Action = 'fix')) su
ON ds.SupplierID = su.SupplierID
WHEN NOT MATCHED THEN
  INSERT
  VALUES
    (su.SupplierID,
     su.CompanyName,
     su.ContactName,
     su.ContactTitle,
     su.Address,
     su.City,
     su.Region,
     su.PostalCode,
     su.Country,
     su.Phone,
     su.Fax,
     su.Homepage)
WHEN MATCHED THEN
  UPDATE
     SET ds.SupplierID   = su.SupplierID,
         ds.CompanyName  = su.CompanyName,
         ds.ContactName  = su.ContactName,
         ds.ContactTitle = su.ContactTitle,
         ds.Address      = su.Address,
         ds.City         = su.City,
         ds.Region       = su.Region,
         ds.PostalCode   = su.PostalCode,
         ds.Country      = su.Country,
         ds.Phone        = su.Phone,
         ds.Fax          = su.Fax,
         ds.Homepage     = su.Homepage;
GO

MERGE INTO dimSuppliers AS ds
USING (SELECT su.SupplierID,
              su.CompanyName,
              su.ContactName,
              su.ContactTitle,
              su.Address,
              su.City,
              su.Region,
              su.PostalCode,
			  CASE 
              WHEN lower(su.Country) in ('us', 'united states') THEN 'USA' 
              WHEN lower(su.Country) in ('united kingdom', 'britain') THEN 'UK' 
              END AS CorrectedCountry,
              su.Country,
              su.Phone,
              su.Fax,
              su.Homepage
         FROM northwind8.dbo.Suppliers su
        WHERE su.%%physloc%% IN
              (SELECT RowID
                 FROM DQLog
                WHERE DBName = 'northwind8'
                  AND TableName = 'Suppliers'
                  AND RuleNo = 4
                  AND Action = 'fix')) su
ON ds.SupplierID = su.SupplierID
WHEN NOT MATCHED THEN
  INSERT
  VALUES
    (su.SupplierID,
     su.CompanyName,
     su.ContactName,
     su.ContactTitle,
     su.Address,
     su.City,
     su.Region,
     su.PostalCode,
     CorrectedCountry,
     su.Phone,
     su.Fax,
     su.Homepage)
WHEN MATCHED THEN
  UPDATE
     SET ds.SupplierID   = su.SupplierID,
         ds.CompanyName  = su.CompanyName,
         ds.ContactName  = su.ContactName,
         ds.ContactTitle = su.ContactTitle,
         ds.Address      = su.Address,
         ds.City         = su.City,
         ds.Region       = su.Region,
         ds.PostalCode   = su.PostalCode,
         ds.Country      = CorrectedCountry,
         ds.Phone        = su.Phone,
         ds.Fax          = su.Fax,
         ds.Homepage     = su.Homepage;
GO

print '***************************************************************'
print '****** Section 3: Populate DW Fact Tables'
print '***************************************************************'
print 'Populating the fact table from northwind7 and northwind8'
--Add statements below... 
--IMPORTANT! All Data in the fact table MUST satisfy all the defined DQ Rules 
MERGE INTO factOrders fo
USING (SELECT dp.ProductKey,
              dc.CustomerKey,
              ds.SupplierKey,
              dt1.TimeKey AS OrderDatekey,
              dt2.TimeKey AS RequiredDatekey,
              dt3.TimeKey AS ShippedDateKey,
              o.OrderID AS OrderID,
              od.UnitPrice AS UnitPrice,
              od.Quantity AS Qty,
              od.Discount,
              od.UnitPrice * od.Quantity AS TotalPrice,
              ship.CompanyName AS ShipperCompany,
              ship.Phone AS ShipperPhone
         FROM BufferOrdersNW7 o, 
		 northwind7.dbo.[Order Details] od, 
		 northwind7.dbo.shippers ship, 
		 northwind7.dbo.Products p, 
		 northwind7.dbo.Suppliers su, 
		 dimCustomers dc, 
		 dimProducts dp, 
		 dimSuppliers ds, 
		 dimTime dt1, 
		 dimTime dt2, 
		 dimTime dt3
        WHERE o.OrderID = od.OrderID
          AND o.CustomerID = dc.CustomerID COLLATE
        DATABASE_DEFAULT
          AND o.ShipVia = ship.ShipperID
          AND od.ProductID = p.ProductID
          AND p.ProductID = dp.ProductID
          AND p.SupplierID = su.SupplierID
          AND su.SupplierID = ds.SupplierID
          AND dt1.Date = o.OrderDate
          AND dt2.Date = o.RequiredDate
          AND dt3.Date = o.ShippedDate
		  AND od.%%physloc%% NOT IN
              (SELECT RowID
                 FROM DQLog
                WHERE DBName = 'northwind7'
                  AND TableName = 'Order Details'
                  AND RuleNo IN (2, 7)
                  AND Action = 'reject')) temp
ON (temp.ProductKey = fo.ProductKey 
	AND temp.CustomerKey = fo.CustomerKey 
	AND temp.SupplierKey = fo.SupplierKey 
	AND temp.OrderDateKey = fo.OrderDateKey)
WHEN MATCHED THEN
  UPDATE
      SET fo.ProductKey     = temp.ProductKey,
         fo.CustomerKey     = temp.CustomerKey,
         fo.SupplierKey     = temp.SupplierKey,
         fo.OrderDateKey    = temp.OrderDateKey,
         fo.RequiredDateKey = temp.RequiredDateKey,
         fo.ShippedDateKey  = temp.ShippedDateKey,
         fo.OrderID         = temp.OrderID,
         fo.UnitPrice       = temp.UnitPrice,
         fo.Qty             = temp.Qty,
         fo.Discount        = temp.Discount,
         fo.TotalPrice      = temp.TotalPrice,
         fo.ShipperCompany  = temp.ShipperCompany,
         fo.ShipperPhone    = temp.ShipperPhone
WHEN NOT MATCHED THEN
  INSERT
    (ProductKey,
     CustomerKey,
     SupplierKey,
     OrderDateKey,
     RequiredDateKey,
     ShippedDateKey,
     OrderID,
     UnitPrice,
     Qty,
     Discount,
     TotalPrice,
     ShipperCompany,
     ShipperPhone)
  VALUES
    (temp.ProductKey,
     temp.CustomerKey,
     temp.SupplierKey,
     temp.OrderDateKey,
     temp.RequiredDateKey,
     temp.ShippedDateKey,
     temp.OrderID,
     temp.UnitPrice,
     temp.Qty,
     temp.Discount,
     temp.TotalPrice,
     temp.ShipperCompany,
     temp.ShipperPhone);

MERGE INTO factOrders fo
USING (SELECT dp.ProductKey,
              dc.CustomerKey,
              ds.SupplierKey,
              dt1.TimeKey AS OrderDatekey,
              dt2.TimeKey AS RequiredDatekey,
              dt3.TimeKey AS ShippedDateKey,
              o.OrderID AS OrderID,
              od.UnitPrice AS UnitPrice,
              od.Quantity AS Qty,
              od.Discount,
              od.UnitPrice * od.Quantity AS TotalPrice,
              ship.CompanyName AS ShipperCompany,
              ship.Phone AS ShipperPhone
         FROM BufferOrdersNW8 o, 
		 northwind8.dbo.[Order Details] od, 
		 northwind8.dbo.shippers ship, 
		 northwind8.dbo.Products p, 
		 northwind8.dbo.Suppliers su, 
		 dimCustomers dc, 
		 dimProducts dp, 
		 dimSuppliers ds, 
		 dimTime dt1, 
		 dimTime dt2, 
		 dimTime dt3
        WHERE o.OrderID = od.OrderID
          AND o.CustomerID = dc.CustomerID COLLATE
        DATABASE_DEFAULT
          AND o.ShipVia = ship.ShipperID
          AND od.ProductID = p.ProductID
          AND p.ProductID = dp.ProductID
          AND p.SupplierID = su.SupplierID
          AND su.SupplierID = ds.SupplierID
          AND dt1.Date = o.OrderDate
          AND dt2.Date = o.RequiredDate
          AND dt3.Date = o.ShippedDate
		  AND od.%%physloc%% NOT IN
              (SELECT RowID
                 FROM DQLog
                WHERE DBName = 'northwind8'
                  AND TableName = 'Order Details'
                  AND RuleNo IN (2, 7)
                  AND Action = 'reject')) temp
ON (temp.ProductKey = fo.ProductKey 
	AND temp.CustomerKey = fo.CustomerKey 
	AND temp.SupplierKey = fo.SupplierKey 
	AND temp.OrderDateKey = fo.OrderDateKey)
WHEN MATCHED THEN
  UPDATE
      SET fo.ProductKey     = temp.ProductKey,
         fo.CustomerKey     = temp.CustomerKey,
         fo.SupplierKey     = temp.SupplierKey,
         fo.OrderDateKey    = temp.OrderDateKey,
         fo.RequiredDateKey = temp.RequiredDateKey,
         fo.ShippedDateKey  = temp.ShippedDateKey,
         fo.OrderID         = temp.OrderID,
         fo.UnitPrice       = temp.UnitPrice,
         fo.Qty             = temp.Qty,
         fo.Discount        = temp.Discount,
         fo.TotalPrice      = temp.TotalPrice,
         fo.ShipperCompany  = temp.ShipperCompany,
         fo.ShipperPhone    = temp.ShipperPhone
WHEN NOT MATCHED THEN
  INSERT
    (ProductKey,
     CustomerKey,
     SupplierKey,
     OrderDateKey,
     RequiredDateKey,
     ShippedDateKey,
     OrderID,
     UnitPrice,
     Qty,
     Discount,
     TotalPrice,
     ShipperCompany,
     ShipperPhone)
  VALUES
    (temp.ProductKey,
     temp.CustomerKey,
     temp.SupplierKey,
     temp.OrderDateKey,
     temp.RequiredDateKey,
     temp.ShippedDateKey,
     temp.OrderID,
     temp.UnitPrice,
     temp.Qty,
     temp.Discount,
     temp.TotalPrice,
     temp.ShipperCompany,
     temp.ShipperPhone);


print '***************************************************************'
print '****** Section 4: Counting rows of OLTP and DW Tables'
print '***************************************************************'
print 'Checking Number of Rows of each table in the source databases and the DW Database'
-- Write SQL queries to get answers to fill in the information below
SELECT count(*) AS northwind7_Customers_Rows FROM northwind7.dbo.Customers;
SELECT count(*) AS northwind8_Customers_Rows FROM northwind8.dbo.Customers;
SELECT count(*) AS northwind7_Products_Rows FROM northwind7.dbo.Products;
SELECT count(*) AS northwind8_Products_Rows FROM northwind8.dbo.Products;
SELECT count(*) AS northwind7_Suppliers_Rows FROM northwind7.dbo.Suppliers;
SELECT count(*) AS northwind8_Suppliers_Rows FROM northwind8.dbo.Suppliers;
SELECT count(*) AS northwind7_Orders_Rows FROM northwind7.dbo.Orders o join northwind7.dbo.[Order Details] od on o.OrderID = od.OrderID;
SELECT count(*) AS northwind8_Orders_Rows FROM northwind8.dbo.Orders o join northwind8.dbo.[Order Details] od on o.OrderID = od.OrderID;
SELECT count(*) AS dimCustomers_Rows FROM dimCustomers;
SELECT count(*) AS dimProducts_Rows FROM dimProducts;
SELECT count(*) AS dimSuppliers_Rows FROM dimSuppliers;
SELECT count(*) AS factOrders_Rows FROM factOrders;
GO
-- ****************************************************************************
-- FILL IN THE ##### 
-- ****************************************************************************
-- Source table					Northwind7	Northwind8	Target table 	DW	
-- ****************************************************************************
-- Customers					13			78			dimCustomers	91
-- Products						77			77			dimProducts		76
-- Suppliers					29			29			dimSuppliers	29
-- Orders join [Order Details] 	352			1801		factOrders		2096
-- ****************************************************************************
--Add statements below

print '***************************************************************'
print '****** Section 5: Validating DW Data'
print '***************************************************************'
print 'A: Validating Data in dimension tables'
print 'Validating dimCustomers data on CustomerID'
SELECT *
  FROM dimCustomers dc
 WHERE dc.CustomerID NOT IN
       (SELECT CustomerID COLLATE DATABASE_DEFAULT
          FROM northwind7.dbo.Customers c
         WHERE dc.CustomerID = c.CustomerID COLLATE DATABASE_DEFAULT
        UNION
        SELECT CustomerID COLLATE DATABASE_DEFAULT
          FROM northwind8.dbo.Customers c
         WHERE dc.CustomerID = c.CustomerID COLLATE DATABASE_DEFAULT)

print 'Validating dimProducts data on ProductID'
SELECT *
  FROM dimProducts dp
 WHERE dp.ProductID NOT IN
       (SELECT ProductID
          FROM northwind7.dbo.Products p
         WHERE dp.ProductID = p.ProductID
        UNION
        SELECT ProductID
          FROM northwind8.dbo.Products p
         WHERE dp.ProductID = p.ProductID)

print 'Validating dimProducts data on CategoryName'
SELECT *
  FROM dimProducts dp
 WHERE dp.CategoryName NOT IN
       (SELECT CategoryName COLLATE DATABASE_DEFAULT
          FROM northwind7.dbo.Products p, northwind7.dbo.Categories c
         WHERE p.CategoryID = c.CategoryID
           AND dp.ProductID = p.ProductID
        UNION
        SELECT CategoryName COLLATE DATABASE_DEFAULT
          FROM northwind8.dbo.Products p, northwind8.dbo.Categories c
         WHERE p.CategoryID = c.CategoryID
           AND dp.ProductID = p.ProductID)

print 'Validating dimSuppliers data on SupplierID'
SELECT *
  FROM dimSuppliers ds
 WHERE ds.SupplierID NOT IN
       (SELECT SupplierID
          FROM northwind7.dbo.Suppliers s
         WHERE ds.SupplierID = s.SupplierID
        UNION
        SELECT SupplierID
          FROM northwind8.dbo.Suppliers s
         WHERE ds.SupplierID = s.SupplierID)

print 'B: Validating Data in the fact table'
print 'Validating factOrders data on FKs and PK'
SELECT *
  FROM factOrders   f,
       dimProducts  dp,
       dimCustomers dc,
	   dimSuppliers ds,
       dimTime      dt1,
       dimTime      dt2,
	   dimTime      dt3
 WHERE f.CustomerKey = dc.CustomerKey
   AND f.ProductKey = dp.ProductKey
   AND f.SupplierKey = ds.SupplierKey
   AND f.OrderDateKey = dt1.TimeKey
   AND f.RequiredDatekey = dt2.TimeKey
   AND f.ShippedDateKey = dt3.TimeKey
   AND NOT EXISTS
 (SELECT o.OrderID
          FROM BufferOrdersNW7 o, northwind7.dbo.[Order Details] od
         WHERE o.OrderID = od.OrderID
           AND f.OrderID = o.OrderID
           AND dp.ProductID = od.ProductID
           AND dc.CustomerID = o.CustomerID COLLATE DATABASE_DEFAULT
           AND dt1.Date = o.OrderDate
           AND dt2.Date = o.RequiredDate
		   AND dt3.Date = o.ShippedDate
        UNION
        SELECT o.OrderID
          FROM BufferOrdersNW8 o, northwind8.dbo.[Order Details] od
         WHERE o.OrderID = od.OrderID
           AND f.OrderID = o.OrderID
           AND dp.ProductID = od.ProductID
           AND dc.CustomerID = o.CustomerID COLLATE DATABASE_DEFAULT
           AND dt1.Date = o.OrderDate
           AND dt2.Date = o.RequiredDate
		   AND dt3.Date = o.ShippedDate)

print 'Validating factOrders data on attributes'
SELECT *
  FROM factOrders   f,
       dimProducts  dp,
       dimCustomers dc,
	   dimSuppliers ds,
       dimTime      dt1,
       dimTime      dt2,
	   dimTime      dt3
 WHERE f.CustomerKey = dc.CustomerKey
   AND f.ProductKey = dp.ProductKey
   AND f.SupplierKey = ds.SupplierKey
   AND f.OrderDateKey = dt1.TimeKey
   AND f.RequiredDatekey = dt2.TimeKey
   AND f.ShippedDateKey = dt3.TimeKey
   AND NOT EXISTS (SELECT o.OrderID 
          FROM BufferOrdersNW7 o, northwind7.dbo.[Order Details ] od
         WHERE o.OrderID = od.OrderID
           AND f.OrderID = o.OrderID
           AND dp.ProductID = od.ProductID
           AND dc.CustomerID = o.CustomerID COLLATE DATABASE_DEFAULT
           AND dt1.Date = o.OrderDate
           AND dt2.Date = o.RequiredDate
		   AND dt3.Date = o.ShippedDate
           AND f.UnitPrice = od.UnitPrice
           AND f.Qty = od.Quantity
           AND f.Discount = od.Discount 
		   AND f.TotalPrice = od.UnitPrice * od.Quantity
        UNION
        SELECT o.OrderID 
          FROM BufferOrdersNW8 o, northwind8.dbo.[Order Details] od
         WHERE o.OrderID = od.OrderID
           AND f.OrderID = o.OrderID
           AND dp.ProductID = od.ProductID
           AND dc.CustomerID = o.CustomerID COLLATE DATABASE_DEFAULT
           AND dt1.Date = o.OrderDate
           AND dt2.Date = o.RequiredDate
           AND dt3.Date = o.ShippedDate
           AND f.UnitPrice = od.UnitPrice
           AND f.Qty = od.Quantity
           AND f.Discount = od.Discount
		   AND f.TotalPrice = od.UnitPrice * od.Quantity)

print '***************************************************************'
print 'My Northwind DW creation with data quality assurance is now completed'
print '***************************************************************'