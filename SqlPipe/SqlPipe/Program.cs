using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Transactions;
using static Clause;
using static Extensions;

#nullable enable

var executor = new Executor("data source=DESKTOP-5R95BQP;initial catalog=Test;trusted_connection=true");

var age = 18;
var amount = 200;

var customers = executor.Query(
    SELECT(
        "C.ID as CustomerId",
        "C.FIRST_NAME as CustomerName",
        "L.ID as LoanId",
        "L.AMOUNT as LoanAmount",
        "L.PMT as LoanPmt")
      .FROM("dbo.CLIENTS as C")
      .INNER_JOIN("dbo.LOANS as L")
      .ON("C.ID = L.CLIENT_ID")
      .WHERE("C.AGE > @age AND L.AMOUNT > @amount")
      .ToSql(),
    CreateParams(
        CreateParam("@age", age),
        CreateParam("@amount", amount)),
    r => new
    {
        CustomerId = r.GetValueAs<int>("CustomerId"),
        CustomerName = r.GetValueAs<string>("CustomerName"),
        LoanId = r.GetValueAs<int>("LoanId"),
        LoanAmount = r.GetValueAs<decimal>("LoanAmount"),
        LoanPmt = r.GetValueAsNullable<decimal>("LoanPmt")
    });


Console.WriteLine(string.Join('\n', customers));


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

public sealed class Disposable<T> where T : IDisposable
{
    private readonly Func<T> _factory;

    internal Disposable(Func<T> factory) =>
        _factory = factory;

    public void Use(Action<T> action)
    {
        using T x = _factory();
        action(x);
    }

    public TResult Use<TResult>(Func<T, TResult> map)
    {
        using var x = _factory();
        return map(x);
    }
}

public static class Disposable
{
    public static Disposable<T> Of<T>(Func<T> factory)
        where T : IDisposable => new(factory);
}

public sealed class Executor
{
    private readonly string _connectionString;

    public Executor(string connectionString) =>
        _connectionString = connectionString;

    private IDbConnection ResolveConnection()
    {
        return EnsureOpen(new SqlConnection(_connectionString));

        static IDbConnection EnsureOpen(IDbConnection connection)
        {
            if (connection.State is not ConnectionState.Open)
                connection.Open();

            return connection;
        }
    }

    private static IDbCommand CreateCommand(IDbConnection connection, string cmd) =>
        new SqlCommand(cmd, connection as SqlConnection);

    private bool ExecuteNonQuery(
        string cmd,
        CommandType commandType,
        SqlParameter[] sqlParameters) =>
        Disposable.Of(ResolveConnection)
                  .Use(c => Disposable.Of(() => CreateCommand(c, cmd))
                                      .Use(c =>
                                      {
                                          c.CommandType = commandType;

                                          Array.ForEach(sqlParameters, p => c.Parameters.Add(p));

                                          return c.ExecuteNonQuery() > 0;
                                      }));

    private TResult? ExecuteQuery<TResult>(
        string cmd,
        CommandBehavior commandBehavior,
        SqlParameter[] sqlParameters,
        Func<IDataReader, TResult?> map) =>
        Disposable.Of(ResolveConnection)
                  .Use(c => Disposable.Of(() => CreateCommand(c, cmd))
                                      .Use(c =>
                                      {
                                          Array.ForEach(sqlParameters, p => c.Parameters.Add(p));

                                          return Disposable.Of(() => c.ExecuteReader(commandBehavior))
                                                           .Use(map);
                                      }));

    public TResult? QuerySingle<TResult>(
        string sql,
        SqlParameter[] sqlParameters,
        Func<IDataRecord, TResult> map) =>
        ExecuteQuery(
            sql,
            CommandBehavior.SingleRow,
            sqlParameters,
            r => r.Read() ? map(r) : default);

    public IList<TResult>? Query<TResult>(
        string sql,
        SqlParameter[] sqlParameters,
        Func<IDataRecord, TResult> map) =>
        ExecuteQuery(
            sql,
            CommandBehavior.Default,
            sqlParameters,
            r => r.AsEnumerable()
                  .Select(map)
                  .ToList());

    public bool Procedure(string name, params SqlParameter[] sqlParameters) =>
        ExecuteNonQuery(name, CommandType.StoredProcedure, sqlParameters);

    public bool Text(string sql, params SqlParameter[] sqlParameters) =>
        ExecuteNonQuery(sql, CommandType.Text, sqlParameters);

    public bool Transaction(Action<Executor> action) =>
        Disposable.Of(() => new TransactionScope())
                  .Use(t =>
                  {
                      try
                      {
                          action(this);

                          t.Complete();

                          return true;
                      }
                      catch { return false; }
                  });
}

public abstract record Clause
{
    public sealed record Select(Clause? Predecessor, params string[] Values) : Clause;
    public sealed record Top(Clause Predecessor, int Value) : Clause;
    public sealed record From(Clause Predecessor, string Value) : Clause;
    public sealed record Where(Clause Predecessor, string Value) : Clause;
    public sealed record OrderBy(Clause Predecessor, string Value) : Clause;
    public sealed record InnerJoin(Clause Predecessor, string Value) : Clause;
    public sealed record LeftJoin(Clause Predecessor, string Value) : Clause;
    public sealed record RightJoin(Clause Predecessor, string Value) : Clause;
    public sealed record FullOuterJoin(Clause Predecessor, string Value) : Clause;
    public sealed record On(Clause Predecessor, string Value) : Clause;
    public sealed record GroupBy(Clause Predecessor, string Value) : Clause;
    public sealed record Having(Clause Predecessor, string Value) : Clause;
    public sealed record Union(Clause Predecessor) : Clause;
}

public static class LessUsableExtensions
{
    public static string ToPrettySql(this Clause self) => self switch
    {
        Select(null, { Length: 0 }) => "SELECT *",
        Select(null, var xs) => $"SELECT {string.Join(", ", xs)}",
        Select(var p, { Length: 0 }) => p.ToPrettySql() + "\nSELECT *",
        Select(var p, var xs) => p.ToPrettySql() + $"\nSELECT {string.Join(',', xs)}",
        Top(var p, var v) => p.ToSql().Replace("SELECT", $"SELECT TOP ({v})"),
        From(var p, var v) => p.ToPrettySql() + $" FROM {v}",
        Where(var p, var v) => p.ToPrettySql() + $"\nWHERE {v}",
        OrderBy(var p, var v) => p.ToPrettySql() + $"\nORDER BY {v}",
        InnerJoin(var p, var v) => p.ToPrettySql() + $"\nINNER JOIN {v}",
        LeftJoin(var p, var v) => p.ToPrettySql() + $"\nLEFT JOIN {v}",
        RightJoin(var p, var v) => p.ToPrettySql() + $"\nRIGHT JOIN {v}",
        FullOuterJoin(var p, var v) => p.ToPrettySql() + $"\nFULL OUTER JOIN {v}",
        On(var p, var v) => p.ToPrettySql() + $"\nON {v}",
        GroupBy(var p, var v) => p.ToPrettySql() + $"\nGROUP BY {v}",
        Having(var p, var v) => p.ToSql() + $"\nHAVING {v}",
        Union(var p) => p.ToPrettySql() + $"\nUNION",
        _ => throw new NotImplementedException(nameof(self))
    };

    public static string ToPrettySql2(this Clause self)
    {
        var keywords = new[] { "FROM", "WHERE", "ORDER BY", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL OUTER JOIN", "ON", "GROUP BY", "UNION" };

        return keywords.Aggregate(
            self.ToSql(),
            (acc, item) => acc.Contains(item) ? acc.Replace(item, $"\n{item}") : acc);
    }

    public static T Tap<T>(this T self)
    {
        Console.WriteLine(self);
        return self;
    }
}

public static class Extensions
{
    #region Executor

    public static TResult? QuerySingle<TResult>(
        this Executor self,
        string sql,
        Func<IDataRecord, TResult> map) =>
        self.QuerySingle(sql, Array.Empty<SqlParameter>(), map);

    public static IList<TResult>? Query<TResult>(
        this Executor self,
        string sql,
        Func<IDataRecord, TResult> map) =>
        self.Query(sql, Array.Empty<SqlParameter>(), map);

    public static bool INSERT_INTO(
        this Executor self,
        string tableName,
        params SqlParameter[] sqlParameters)
    {
        var sqlBuilder = new StringBuilder($"INSERT INTO {tableName} ")
            .Append($"({string.Join(',', sqlParameters.Select(p => p.ParameterName))}) ");

        sqlParameters = Array.ConvertAll(sqlParameters, p => { p.ParameterName = '@' + p.ParameterName; return p; });

        var sql = sqlBuilder.Append($"VALUES ({string.Join(',', sqlParameters.Select(p => p.ParameterName))})")
                            .ToString();

        return self.Text(sql, sqlParameters);
    }

    public static bool UPDATE(
        this Executor self,
        string tableName,
        SqlParameter[] sqlParameters,
        string where) =>
        self.Text(
            $"UPDATE {tableName} SET {string.Join(',', sqlParameters.Select(p => $"{p.ParameterName} = @{p.ParameterName}"))} WHERE {where}",
            Array.ConvertAll(
                sqlParameters,
                p => { p.ParameterName = '@' + p.ParameterName; return p; }));

    public static bool DELETE_FROM(
        this Executor self,
        string tableName,
        string where) =>
        self.Text($"DELETE FROM {tableName} WHERE {where}");

    #endregion

    #region Clause

    public static Select SELECT(params string[] values) => new(null, values);
    public static Select SELECT(this Union self, params string[] values) => new(self, values);
    public static From FROM(this Select self, string value) => new(self, value);
    public static From FROM(this Top self, string value) => new(self, value);
    public static Top TOP(this Select self, int value) => new(self, value);
    public static Where WHERE(this From self, string value) => new(self, value);
    public static Where WHERE(this On self, string value) => new(self, value);
    public static OrderBy ORDER_BY(this From self, string value) => new(self, value);
    public static OrderBy ORDER_BY(this Where self, string value) => new(self, value);
    public static OrderBy ORDER_BY(this On self, string value) => new(self, value);
    public static OrderBy ORDER_BY(this GroupBy self, string value) => new(self, value);
    public static OrderBy ORDER_BY(this Having self, string value) => new(self, value);
    public static InnerJoin INNER_JOIN(this From self, string value) => new(self, value);
    public static LeftJoin LEFT_JOIN(this From self, string value) => new(self, value);
    public static RightJoin RIGHT_JOIN(this From self, string value) => new(self, value);
    public static FullOuterJoin FULL_OUTER_JOIN(this From self, string value) => new(self, value);
    public static InnerJoin INNER_JOIN(this On self, string value) => new(self, value);
    public static LeftJoin LEFT_JOIN(this On self, string value) => new(self, value);
    public static RightJoin RIGHT_JOIN(this On self, string value) => new(self, value);
    public static FullOuterJoin FULL_OUTER_JOIN(this On self, string value) => new(self, value);
    public static On ON(this InnerJoin self, string value) => new(self, value);
    public static On ON(this LeftJoin self, string value) => new(self, value);
    public static On ON(this RightJoin self, string value) => new(self, value);
    public static On ON(this FullOuterJoin self, string value) => new(self, value);
    public static GroupBy GROUP_BY(this From self, string value) => new(self, value);
    public static GroupBy GROUP_BY(this Where self, string value) => new(self, value);
    public static GroupBy GROUP_BY(this On self, string value) => new(self, value);
    public static Having HAVING(this GroupBy self, string value) => new(self, value);
    public static Union UNION(this Where self) => new(self);
    public static Union UNION(this From self) => new(self);
    public static Union UNION(this On self) => new(self);

    public static string ToSql(this Clause self) => self switch
    {
        Select(null, { Length: 0 }) => "SELECT *",
        Select(null, var xs) => $"SELECT {string.Join(", ", xs)}",
        Select(var p, { Length: 0 }) => $"{p.ToSql()} SELECT *",
        Select(var p, var xs) => $"{p.ToSql()} SELECT {string.Join(',', xs)}",
        Top(var p, var v) => p.ToSql().Replace("SELECT", $"SELECT TOP ({v})"),
        From(var p, var v) => $"{p.ToSql()} FROM {v}",
        Where(var p, var v) => $"{p.ToSql()} WHERE {v}",
        OrderBy(var p, var v) => $"{p.ToSql()} ORDER BY {v}",
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

    #endregion

    #region IDataReader, IDataRecord

    public static IEnumerable<IDataRecord> AsEnumerable(this IDataReader self)
    {
        while (self.Read())
            yield return self;
    }

    public static T GetValueAs<T>(this IDataRecord self, string columnName) =>
        (T)self.GetValue(self.GetOrdinal(columnName));

    public static T? GetValueAsNullable<T>(this IDataRecord self, string columnName) where T : struct =>
        self.IsDBNull(self.GetOrdinal(columnName))
            ? default
            : (T?)self.GetValue(self.GetOrdinal(columnName));

    #endregion

    #region SqlParameter

    public static SqlParameter[] CreateParams(params SqlParameter[] sqlParameters) => sqlParameters;
    private static SqlParameter CreateParam(string parameterName, object value, DbType dbType) => new(parameterName, value) { DbType = dbType };
    public static SqlParameter CreateParam(string parameterName, byte value) => CreateParam(parameterName, value, DbType.Byte);
    public static SqlParameter CreateParam(string parameterName, short value) => CreateParam(parameterName, value, DbType.Int16);
    public static SqlParameter CreateParam(string parameterName, int value) => CreateParam(parameterName, value, DbType.Int32);
    public static SqlParameter CreateParam(string parameterName, long value) => CreateParam(parameterName, value, DbType.Int64);
    public static SqlParameter CreateParam(string parameterName, float value) => CreateParam(parameterName, value, DbType.Double);
    public static SqlParameter CreateParam(string parameterName, double value) => CreateParam(parameterName, value, DbType.Double);
    public static SqlParameter CreateParam(string parameterName, decimal value) => CreateParam(parameterName, value, DbType.Decimal);
    public static SqlParameter CreateParam(string parameterName, string value) => CreateParam(parameterName, value, DbType.String);

    #endregion
}