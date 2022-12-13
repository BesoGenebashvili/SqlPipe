using System.Data;
using static Extensions;

#nullable disable

var executor = new Executor("data source=DESKTOP-5R95BQP;initial catalog=Test;trusted_connection=true");

// Create database
await executor.TextAsync(@"
IF NOT EXISTS(SELECT * FROM sysobjects WHERE name='CLIENTS' AND xtype = 'U')
    CREATE TABLE dbo.CLIENTS(
        ID INT NOT NULL IDENTITY(1, 1) PRIMARY KEY,
        FIRST_NAME VARCHAR(100) NOT NULL,
        LAST_NAME VARCHAR(100) NOT NULL,
        AGE TINYINT NULL,
        IS_ACTIVE BIT NOT NULL)");

// Insert
var clientIdOutParam = SqlOutParam("@IDENTITY", SqlDbType.Int);
var inserted = await executor.INSERT_INTO(
                          "dbo.CLIENTS",
                          SqlParams(
                              SqlParam("@firstName", "giorgi", "FIRST_NAME"),
                              SqlParam("@lastName", "giorgadze", "LAST_NAME"),
                              SqlParam("@age", 10, "AGE"),
                              SqlParam("@isActive", true, "IS_ACTIVE"),
                              clientIdOutParam));

if (inserted) Console.WriteLine($"Inserted client `{clientIdOutParam.Value}`");
else Console.WriteLine("Insert was unsuccessful");

// Update
var updated = await executor.UPDATE(
                        "dbo.CLIENTS",
                        SqlParams(
                            SqlParam("@age", 15, "AGE"),
                            SqlParam("@id", 1)),
                        "Id = @id");

if (updated) Console.WriteLine($"Updated client `{1}`");
else Console.WriteLine("Update was unsuccessful");

// Query single
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

// Query list
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
                     SqlParams(
                         SqlParam("isActive", true),
                         SqlParam("@idCount", 5)),
                     r => new
                     {
                         Id = r.GetValueAs<int>("ID"),
                         FullName = r.GetValueAs<string>("FULL_NAME"),
                         Age = r.GetValueAsNullable<byte>("AGE"),
                         IsActive = r.GetValueAs<bool>("IS_ACTIVE")
                     });

Console.WriteLine(string.Join('\n', list));