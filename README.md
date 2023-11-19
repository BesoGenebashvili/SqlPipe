# SqlPipe
SqlPipe is a C# DSL that simplifies SQL tasks while safeguarding against writing faulty queries, ensuring reliable database operations for executing, inserting, updating, and retrieving data

[![Downloads](https://badgen.net/nuget/dt/SqlPipe)](https://www.nuget.org/packages/SqlPipe/)
[![NuGet](https://badgen.net/nuget/v/SqlPipe)](https://www.nuget.org/packages/SqlPipe/)
[![License](https://badgen.net/github/license/BesoGenebashvili/SqlPipe)](https://github.com/BesoGenebashvili/SqlPipe/blob/main/LICENSE.md)

<img src="https://user-images.githubusercontent.com/52665934/207603933-864b0d4b-5c8d-41a0-afc9-5ecb49e011f0.png" width="300">

### SQL Query Generation Example

```csharp
var sql = SELECT()
              .TOP(10)
              .FROM("dbo.CLIENTS c")
              .INNER_JOIN("dbo.ORDERS o")
              .ON("c.ID = o.CLIENT_ID")
              .WHERE("c.IS_ACTIVE = @isActive")
              .ORDER_BY("c.ID")
              .ORDER_BY_DESC("o.DATE")
              .ToPrettySql();
```

#### Generated SQL Query:

```sql
SELECT TOP (10) * 
FROM dbo.CLIENTS c 
INNER JOIN dbo.ORDERS o 
ON c.ID = o.CLIENT_ID 
WHERE c.IS_ACTIVE = @isActive 
ORDER BY c.ID, o.DATE DESC;
```

### Getting Started
To use SqlPipe, import the necessary namespaces and create an executor to interact with the database:

```csharp
using SqlPipe;
using static SqlPipe.Extensions;

var executor = new Executor("data source=_;initial catalog=SqlPipeDocs;trusted_connection=true");
```

### Creating a Table

```csharp
await executor.TextAsync(
    """ 
    CREATE TABLE dbo.CLIENTS(
        ID         INT          NOT NULL IDENTITY(1, 1) PRIMARY KEY,
        FIRST_NAME VARCHAR(100) NOT NULL,
        LAST_NAME  VARCHAR(100) NOT NULL,
        AGE        TINYINT      NULL,
        IS_ACTIVE  BIT          NOT NULL);
    """);
```

### Inserting Data

```csharp
var clientIdOutParam = SqlOutParam("@IDENTITY", SqlDbType.Int);
var inserted = await executor.INSERT_INTO(
    "dbo.CLIENTS",
    SqlParams(
        SqlParam("@firstName", "Beso", "FIRST_NAME"),
        SqlParam("@lastName", "Genebashvili", "LAST_NAME"),
        SqlParam("@age", 24, "AGE"),
        SqlParam("@isActive", true, "IS_ACTIVE"),
        clientIdOutParam));

// Check if insertion was successful
if (inserted) Console.WriteLine($"Inserted client `{clientIdOutParam.Value}`");
else Console.WriteLine("Insert was unsuccessful");
```

### Updating Data

```csharp
var updated = await executor.UPDATE(
    "dbo.CLIENTS",
    SqlParams(
        SqlParam("@id", 1),
        SqlParam("@age", 15, "AGE")),
    "Id = @id");

// Check if update was successful
if (updated) Console.WriteLine($"Updated client `{1}`");
else Console.WriteLine("Update was unsuccessful");
```

### Querying Single Record

```csharp
var querySingleSql = SELECT("ID as Id",
                            "FIRST_NAME as FirstName",
                            "LAST_NAME as LastName")
                        .FROM("dbo.CLIENTS")
                        .WHERE("Id = @id")
                        .ToSql();

var singleRecord = await executor.QuerySingleAsync(
    querySingleSql,
    SqlParams(SqlParam("id", 1)),
    r => new
    {
        Id = r.GetValueAs<int>("Id"),
        FirstName = r.GetValueAs<string>("FirstName"),
        LastName = r.GetValueAs<string>("LastName")
    });

Console.WriteLine(singleRecord);
```

### Querying a List

```csharp
var queryListSql = SELECT("*", "FIRST_NAME + ' ' + LAST_NAME as FULL_NAME")
                       .TOP(10)
                       .FROM("dbo.CLIENTS")
                       .WHERE("IS_ACTIVE = @isActive")
                       .ORDER_BY("ID")
                       .ORDER_BY_DESC("AGE")
                       .ORDER_BY_ASC("FULL_NAME")
                       .ToPrettySql();

var list = await executor.QueryAsync(
    queryListSql,
    SqlParams(SqlParam("isActive", true)),
    r => new
    {
        Id = r.GetValueAs<int>("ID"),
        FullName = r.GetValueAs<string>("FULL_NAME"),
        Age = r.GetValueAsNullable<byte>("AGE"),
        IsActive = r.GetValueAs<bool>("IS_ACTIVE")
    });

Console.WriteLine(string.Join('\n', list));
```

## Installation

```
Install-Package SqlPipe
```

```
dotnet add package SqlPipe
```
