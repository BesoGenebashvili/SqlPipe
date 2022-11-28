﻿using System.Data;
using System.Data.SqlClient;
using System.Transactions;
using static Clause;
using static Extensions;

#nullable enable

var executor = new Executor("data source=DESKTOP-5R95BQP;initial catalog=Test;trusted_connection=true");

var sql = SELECT().FROM("dbo.USERS")
                  .WHERE("USERNAME = @username")
                  .GROUP_BY("USERNAME")
                  .HAVING("ID = @id")
                  .ORDER_BY("ID")
                  .ToPrettySql();

var results = await executor.QueryAsync(
    sql,
    SqlParams(
        SqlParam("@username", "saba"),
        SqlParam("@id", 1)),
    x => new
    {
        Id = x.GetValueAs<int>("ID"),
        Username = x.GetValueAs<string>("USERNAME"),
        Password = x.GetValueAs<string>("PASSWORD")
    });

Console.WriteLine(
    string.Join('\n', results));

//await executor.INSERT_INTO(
//    "dbo.USERS",
//    SqlParams(
//        SqlParam("@username", "saba", "USERNAME"),
//        SqlParam("@pass", "saba123", "PASSWORD")));

//await executor.TextAsync(
//    @"
//CREATE TABLE dbo.USERS(
//ID INT NOT NULL IDENTITY(1, 1) PRIMARY KEY,
//USERNAME VARCHAR(100) NOT NULL,
//PASSWORD VARCHAR(100) NOT NULL)");

//await executor.TextAsync(
//    @"
//CREATE TABLE dbo.TODOS(
//ID INT NOT NULL IDENTITY(1, 1) PRIMARY KEY,
//USER_ID INT NOT NULL FOREIGN KEY REFERENCES USERS(ID),
//TITLE VARCHAR(100) NOT NULL,
//DESCRIPTION VARCHAR(MAX) NOT NULL,
//IS_DONE BIT NOT NULL,
//CATEGORY TINYINT NOT NULL,
//NOTE VARCHAR(MAX) NOT NULL)");

//await executor.TextAsync(
//    @"
//CREATE TABLE dbo.TODO_FILES(
//ID INT NOT NULL IDENTITY(1, 1) PRIMARY KEY,
//TODO_ID INT NOT NULL FOREIGN KEY REFERENCES TODOS(ID),
//PATH VARCHAR(200) NOT NULL)");

//var age = 18;
//var amount = 200;

//var customers = await executor.QueryAsync(
//    SELECT(
//        "C.ID as CustomerId",
//        "C.FIRST_NAME as CustomerName",
//        "L.ID as LoanId",
//        "L.AMOUNT as LoanAmount",
//        "L.PMT as LoanPmt")
//      .FROM("dbo.CLIENTS as C")
//      .INNER_JOIN("dbo.LOANS as L")
//      .ON("C.ID = L.CLIENT_ID")
//      .WHERE("C.AGE > @age AND L.AMOUNT > @amount")
//      .ToSql(),
//    SqlParams(
//        SqlParam("@age", age),
//        SqlParam("@amount", amount)),
//    r => new
//    {
//        CustomerId = r.GetValueAs<int>("CustomerId"),
//        CustomerName = r.GetValueAs<string>("CustomerName"),
//        LoanId = r.GetValueAs<int>("LoanId"),
//        LoanAmount = r.GetValueAs<decimal>("LoanAmount"),
//        LoanPmt = r.GetValueAsNullable<decimal>("LoanPmt")
//    });


//Console.WriteLine(string.Join('\n', customers));

//executor.INSERT_INTO(
//    "dbo.LOANS",
//    new SqlParameter[]
//    {
//        new("CLIENT_ID", 4),
//        new("AMOUNT", 130m),
//        new("TERM_BEGIN", new DateTime(2022, 5, 1)),
//        new("TERM_END", new DateTime(2022, 7, 1))
//    });

//var clients = executor.Query(
//    SELECT().FROM("dbo.CLIENTS")
//            .ToSql(),
//    r => new
//    {
//        Id = r.GetValueAs<int>("ID"),
//        Name = r.GetValueAs<string>("FIRST_NAME"),
//        Age = r.GetValueAs<byte>("AGE")
//    });

//Console.WriteLine(string.Join('\n', clients));

//executor.UPDATE(
//    "dbo.CLIENTS",
//    new SqlParameter[]
//    {
//        new("FIRST_NAME", "B")
//    },
//    "ID = 6");


//Console.WriteLine("Hello, World!");

public sealed class Executor
{
    private readonly string _connectionString;

    public Executor(string connectionString) =>
        _connectionString = connectionString;

    private async Task<bool> ExecuteNonQueryAsync(
        string command,
        CommandType commandType,
        SqlParameter[] sqlParameters)
    {
        using var sqlConnection = new SqlConnection(_connectionString);
        using var sqlCommand = new SqlCommand(command, sqlConnection);

        sqlCommand.CommandType = commandType;
        sqlCommand.Parameters.AddRange(sqlParameters);

        await sqlConnection.OpenAsync();
        return await sqlCommand.ExecuteNonQueryAsync() > 0;
    }

    private async Task<TResult> ExecuteQueryAsync<TResult>(
        string command,
        CommandBehavior commandBehavior,
        SqlParameter[] sqlParameters,

        Func<IDataReader, TResult> map)
    {
        using var sqlConnection = new SqlConnection(_connectionString);
        using var sqlCommand = new SqlCommand(command, sqlConnection);

        sqlCommand.Parameters.AddRange(sqlParameters);

        await sqlConnection.OpenAsync();
        return map(await sqlCommand.ExecuteReaderAsync(commandBehavior));
    }

    public Task<TResult> QuerySingleAsync<TResult>(
        string sql,
        SqlParameter[] sqlParameters,
        Func<IDataRecord, TResult> map) =>
        ExecuteQueryAsync(
            sql,
            CommandBehavior.SingleRow,
            sqlParameters,
            r => r.Read() ? map(r) : default)!;

    public Task<List<TResult>> QueryAsync<TResult>(
        string sql,
        SqlParameter[] sqlParameters,
        Func<IDataRecord, TResult> map) =>
        ExecuteQueryAsync(
            sql,
            CommandBehavior.Default,
            sqlParameters,
            r => r.AsEnumerable()
                  .Select(map)
                  .ToList());

    public Task<bool> ProcedureAsync(string name, params SqlParameter[] sqlParameters) =>
        ExecuteNonQueryAsync(name, CommandType.StoredProcedure, sqlParameters);

    public Task<bool> TextAsync(string sql, params SqlParameter[] sqlParameters) =>
        ExecuteNonQueryAsync(sql, CommandType.Text, sqlParameters);
}

public abstract record Clause
{
    public sealed record Select(Clause? Predecessor, params string[] Columns) : Clause;
    public sealed record Top(Clause Predecessor, int Value) : Clause;
    public sealed record From(Clause Predecessor, string Table) : Clause;
    public sealed record Where(Clause Predecessor, string Condition) : Clause;
    public sealed record OrderBy(Clause Predecessor, string Column) : Clause;
    public sealed record OrderByAsc(Clause Predecessor, string Column) : Clause;
    public sealed record OrderByDesc(Clause Predecessor, string Column) : Clause;
    public sealed record InnerJoin(Clause Predecessor, string Table) : Clause;
    public sealed record LeftJoin(Clause Predecessor, string Table) : Clause;
    public sealed record RightJoin(Clause Predecessor, string Table) : Clause;
    public sealed record FullOuterJoin(Clause Predecessor, string Table) : Clause;
    public sealed record On(Clause Predecessor, string Condition) : Clause;
    public sealed record GroupBy(Clause Predecessor, string Column) : Clause;
    public sealed record Having(Clause Predecessor, string Condition) : Clause;
    public sealed record Union(Clause Predecessor) : Clause;
}

public static class Extensions
{
    #region Executor

    public static bool Transaction(this Executor self, Action<Executor> action)
    {
        using var transactionScope = new TransactionScope();

        try
        {
            action(self);

            transactionScope.Complete();

            return true;
        }
        catch { return false; }
    }

    public static Task<TResult> QuerySingleAsync<TResult>(
        this Executor self,
        string sql,
        Func<IDataRecord, TResult> map) =>
        self.QuerySingleAsync(sql, Array.Empty<SqlParameter>(), map);

    public static Task<List<TResult>> QueryAsync<TResult>(
        this Executor self,
        string sql,
        Func<IDataRecord, TResult> map) =>
        self.QueryAsync(sql, Array.Empty<SqlParameter>(), map);

    /// <summary>
    /// executes 'INSERT INTO' command
    /// </summary>
    /// <param name="tableName">Table name</param>
    /// <param name="sqlParameters">Provide '@IDENTITY' output parameter for last identity</param>
    /// <returns>'@@ROWCOUNT' > 0</returns>
    public static Task<bool> INSERT_INTO(
        this Executor self,
        string tableName,
        SqlParameter[] sqlParameters)
    {
        var inputParams = sqlParameters.Where(p => p.Direction == ParameterDirection.Input);

        var sql = $@"
INSERT INTO {tableName} ({string.Join(',', inputParams.Select(p => p.SourceColumn))})
VALUES ({string.Join(',', inputParams.Select(p => p.ParameterName))})
{(sqlParameters.Any(p => p.ParameterName == "@IDENTITY") ? "SET @IDENTITY = SCOPE_IDENTITY()" : string.Empty)};";

        return self.TextAsync(sql, sqlParameters);
    }

    public static Task<bool> UPDATE(
        this Executor self,
        string tableName,
        SqlParameter[] sqlParameters,
        string where) =>
        self.TextAsync(
            $@"
UPDATE {tableName}
SET {string.Join(',', sqlParameters.Select(p => $"{p.SourceColumn} = {p.ParameterName}"))}
WHERE {where}",
            sqlParameters);

    public static Task<bool> DELETE_FROM(
        this Executor self,
        string tableName,
        SqlParameter[] sqlParameters,
        string where) =>
        self.TextAsync(
            $"DELETE FROM {tableName} WHERE {where}",
            sqlParameters);

    #endregion

    #region Clause

    public static Select SELECT(params string[] columns) => new(null, columns);
    public static Select SELECT(this Union self, params string[] columns) => new(self, columns);

    public static From FROM(this Select self, string table) => new(self, table);
    public static From FROM(this Top self, string table) => new(self, table);

    public static Top TOP(this Select self, int value) => new(self, value);

    public static Where WHERE(this From self, string condition) => new(self, condition);
    public static Where WHERE(this On self, string condition) => new(self, condition);

    public static OrderBy ORDER_BY(this From self, string column) => new(self, column);
    public static OrderBy ORDER_BY(this Where self, string column) => new(self, column);
    public static OrderBy ORDER_BY(this On self, string column) => new(self, column);
    public static OrderBy ORDER_BY(this GroupBy self, string column) => new(self, column);
    public static OrderBy ORDER_BY(this Having self, string column) => new(self, column);
    public static OrderBy ORDER_BY(this OrderBy self, string column) => new(self, column);
    public static OrderBy ORDER_BY(this OrderByAsc self, string column) => new(self, column);
    public static OrderBy ORDER_BY(this OrderByDesc self, string column) => new(self, column);

    public static OrderByAsc ORDER_BY_ASC(this From self, string column) => new(self, column);
    public static OrderByAsc ORDER_BY_ASC(this Where self, string column) => new(self, column);
    public static OrderByAsc ORDER_BY_ASC(this On self, string column) => new(self, column);
    public static OrderByAsc ORDER_BY_ASC(this GroupBy self, string column) => new(self, column);
    public static OrderByAsc ORDER_BY_ASC(this Having self, string column) => new(self, column);
    public static OrderByAsc ORDER_BY_ASC(this OrderBy self, string column) => new(self, column);
    public static OrderByAsc ORDER_BY_ASC(this OrderByAsc self, string column) => new(self, column);
    public static OrderByAsc ORDER_BY_ASC(this OrderByDesc self, string column) => new(self, column);

    public static OrderByDesc ORDER_BY_DESC(this From self, string column) => new(self, column);
    public static OrderByDesc ORDER_BY_DESC(this Where self, string column) => new(self, column);
    public static OrderByDesc ORDER_BY_DESC(this On self, string column) => new(self, column);
    public static OrderByDesc ORDER_BY_DESC(this GroupBy self, string column) => new(self, column);
    public static OrderByDesc ORDER_BY_DESC(this Having self, string column) => new(self, column);
    public static OrderByDesc ORDER_BY_DESC(this OrderBy self, string column) => new(self, column);
    public static OrderByDesc ORDER_BY_DESC(this OrderByAsc self, string column) => new(self, column);
    public static OrderByDesc ORDER_BY_DESC(this OrderByDesc self, string column) => new(self, column);

    public static InnerJoin INNER_JOIN(this From self, string table) => new(self, table);
    public static InnerJoin INNER_JOIN(this On self, string table) => new(self, table);
    public static LeftJoin LEFT_JOIN(this From self, string table) => new(self, table);
    public static LeftJoin LEFT_JOIN(this On self, string table) => new(self, table);
    public static RightJoin RIGHT_JOIN(this From self, string table) => new(self, table);
    public static RightJoin RIGHT_JOIN(this On self, string table) => new(self, table);
    public static FullOuterJoin FULL_OUTER_JOIN(this From self, string table) => new(self, table);
    public static FullOuterJoin FULL_OUTER_JOIN(this On self, string table) => new(self, table);

    public static On ON(this InnerJoin self, string condition) => new(self, condition);
    public static On ON(this LeftJoin self, string condition) => new(self, condition);
    public static On ON(this RightJoin self, string condition) => new(self, condition);
    public static On ON(this FullOuterJoin self, string condition) => new(self, condition);

    public static GroupBy GROUP_BY(this From self, string column) => new(self, column);
    public static GroupBy GROUP_BY(this Where self, string column) => new(self, column);
    public static GroupBy GROUP_BY(this On self, string column) => new(self, column);

    public static Having HAVING(this GroupBy self, string condition) => new(self, condition);

    public static Union UNION(this Where self) => new(self);
    public static Union UNION(this From self) => new(self);
    public static Union UNION(this On self) => new(self);

    public static string ToSql(this Clause self)
    {
        return self switch
        {
            Select(null, { Length: 0 }) => "SELECT *",
            Select(null, var xs) => $"SELECT {string.Join(", ", xs)}",
            Select(var p, { Length: 0 }) => $"{p.ToSql()} SELECT *",
            Select(var p, var xs) => $"{p.ToSql()} SELECT {string.Join(',', xs)}",
            Top(var p, var v) => p.ToSql().Replace("SELECT", $"SELECT TOP ({v})"),
            From(var p, var v) => $"{p.ToSql()} FROM {v}",
            Where(var p, var v) => $"{p.ToSql()} WHERE {v}",
            OrderBy(var p, var v) => AppendOrderBy(p.ToSql(), v),
            OrderByAsc(var p, var v) => AppendOrderBy(p.ToSql(), v, true),
            OrderByDesc(var p, var v) => AppendOrderBy(p.ToSql(), v, false),
            InnerJoin(var p, var v) => $"{p.ToSql()} INNER JOIN {v}",
            LeftJoin(var p, var v) => $"{p.ToSql()} LEFT JOIN {v}",
            RightJoin(var p, var v) => $"{p.ToSql()} RIGHT JOIN {v}",
            FullOuterJoin(var p, var v) => $"{p.ToSql()} FULL OUTER JOIN {v}",
            On(var p, var v) => $"{p.ToSql()} ON {v}",
            GroupBy(var p, var v) => $"{p.ToSql()} GROUP BY {v}",
            Having(var p, var v) => $"{p.ToSql()} HAVING {v}",
            Union(var p) => $"{p.ToSql()} UNION",
            _ => throw new NotImplementedException(nameof(self))
        };

        static string AppendOrderBy(
            string sql,
            string column,
            bool? isAsc = default)
        {
            var columnAndOrder = isAsc switch
            {
                true => $"{column} ASC",
                false => $"{column} DESC",
                _ => column
            };

            return sql.Contains("ORDER BY")
                      ? $"{sql[..^1]}, {columnAndOrder};"
                      : $"{sql} ORDER BY {columnAndOrder};";
        }
    }

    public static string ToPrettySql(this Clause self) =>
        new[] { "FROM",
                "WHERE",
                "ORDER BY",
                "INNER JOIN",
                "LEFT JOIN",
                "RIGHT JOIN",
                "FULL OUTER JOIN",
                "ON",
                "GROUP BY",
                "UNION" }
            .Aggregate(
                self.ToSql(),
                (acc, item) => acc.Contains(item) ? acc.Replace(item, '\n' + item) : acc);

    #endregion

    #region IDataReader, IDataRecord

    public static IEnumerable<IDataRecord> AsEnumerable(this IDataReader self)
    {
        while (self.Read())
            yield return self;
    }

    public static T GetValueAs<T>(this IDataRecord self, string columnName) =>
        (T)self[columnName];

    public static T? GetValueAsNullable<T>(this IDataRecord self, string columnName) where T : struct =>
        self.IsDBNull(self.GetOrdinal(columnName))
            ? default
            : (T?)self[columnName];

    #endregion

    #region SqlParameter

    public static SqlParameter[] SqlParams(params SqlParameter[] sqlParameters) => sqlParameters;

    private static SqlParameter SqlParam(
        string parameterName,
        object value,
        DbType dbType,
        string? sourceColumn = null) =>
        new()
        {
            ParameterName = parameterName,
            Value = value,
            DbType = dbType,
            SourceColumn = sourceColumn,
        };

    public static SqlParameter SqlOutParam(string parameterName, SqlDbType dbType) => new(parameterName, dbType) { Direction = ParameterDirection.Output };
    public static SqlParameter SqlParam(string parameterName, bool value, string? sourceColumn = null) => SqlParam(parameterName, value, DbType.Boolean, sourceColumn);
    public static SqlParameter SqlParam(string parameterName, byte value, string? sourceColumn = null) => SqlParam(parameterName, value, DbType.Byte, sourceColumn);
    public static SqlParameter SqlParam(string parameterName, short value, string? sourceColumn = null) => SqlParam(parameterName, value, DbType.Int16, sourceColumn);
    public static SqlParameter SqlParam(string parameterName, int value, string? sourceColumn = null) => SqlParam(parameterName, value, DbType.Int32, sourceColumn);
    public static SqlParameter SqlParam(string parameterName, long value, string? sourceColumn = null) => SqlParam(parameterName, value, DbType.Int64, sourceColumn);
    public static SqlParameter SqlParam(string parameterName, float value, string? sourceColumn = null) => SqlParam(parameterName, value, DbType.Double, sourceColumn);
    public static SqlParameter SqlParam(string parameterName, double value, string? sourceColumn = null) => SqlParam(parameterName, value, DbType.Double, sourceColumn);
    public static SqlParameter SqlParam(string parameterName, decimal value, string? sourceColumn = null) => SqlParam(parameterName, value, DbType.Decimal, sourceColumn);
    public static SqlParameter SqlParam(string parameterName, string value, string? sourceColumn = null) => SqlParam(parameterName, value, DbType.String, sourceColumn);

    #endregion
}