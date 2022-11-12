using System.Data;
using System.Data.SqlClient;
using System.Runtime.CompilerServices;
using System.Transactions;
using static Clause;
using static Extensions;

#nullable enable

var executor = new Executor("data source=DESKTOP-5R95BQP;initial catalog=Test;trusted_connection=true");

var sql = SELECT("C.Id as CustomerId",
                 "C.Age as CustomerAge",
                 "L.Amount as LoanAmount",
                 "L.TermStart as LoanTermStart",
                 "L.TermEnd as LoanTermEnd")
              .FROM("dbo.Customers as C")
              .INNER_JOIN("dbo.Loans as L")
              .ON("C.Id == L.CustomerId")
              .WHERE("C.Age >= 18")
              .ORDER_BY("L.Amount")
              .ToSql();

Console.WriteLine(sql);

var xs = executor.Query(sql, Customer.FromDataRecord);

Console.WriteLine("Hello, World!");

public sealed record Customer(
    int Id,
    int? Age,
    decimal LoanAmount,
    DateOnly LoanTermStart,
    DateOnly LoanTermEnd)
{
    public static Customer FromDataRecord(IDataRecord dataRecord) =>
        new(dataRecord.GetValueAs<int>("CustomerId"),
            dataRecord.GetValueAsNullable<int>("CustomerAge"),
            dataRecord.GetValueAs<decimal>("LoanAmount"),
            dataRecord.GetValueAs<DateOnly>("LoanTermStart"),
            dataRecord.GetValueAs<DateOnly>("LoanTermEnd"));
}

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

    private TResult? Query<TResult>(string sql, Func<IDataReader, TResult> map, CommandBehavior commandBehavior) =>
        Disposable.Of(ResolveConnection)
                  .Use(c => Disposable.Of(() => CreateCommand(c, sql))
                                      .Use(c => Disposable.Of(() => c.ExecuteReader(commandBehavior))
                                                          .Use(r => r.Read() ? map(r) : default)));

    public TResult? QuerySingle<TResult>(string sql, Func<IDataRecord, TResult> map) =>
        Query(sql, map, CommandBehavior.SingleRow);

    public IEnumerable<TResult>? Query<TResult>(string sql, Func<IDataRecord, TResult> map) =>
        Query(sql, r => r.AsEnumerable().Select(map), CommandBehavior.Default);

    public bool Procedure(string name, params SqlParameter[] sqlParameters) =>
        Disposable.Of(ResolveConnection)
                  .Use(c => Disposable.Of(() => CreateCommand(c, name))
                                      .Use(c =>
                                      {
                                          c.CommandType = CommandType.StoredProcedure;

                                          sqlParameters.ToList()
                                                       .ForEach(p => c.Parameters.Add(p));

                                          return c.ExecuteNonQuery() > 0;
                                      }));

    public bool Text(string sql, params SqlParameter[] sqlParameters) =>
        Disposable.Of(ResolveConnection)
                  .Use(c => Disposable.Of(() => CreateCommand(c, sql))
                                      .Use(c =>
                                      {
                                          sqlParameters.ToList()
                                                       .ForEach(p => c.Parameters.Add(p));

                                          return c.ExecuteNonQuery() > 0;
                                      }));

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

public static class Extensions
{
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
        Select(var p, { Length: 0 }) => p.ToSql() + " SELECT *",
        Select(var p, var xs) => p.ToSql() + $" SELECT {string.Join(',', xs)}",
        Top(var p, var v) => p.ToSql().AppendAfter("SELECT", $" TOP ({v})"),
        From(var p, var v) => p.ToSql() + $" FROM {v}",
        Where(var p, var v) => p.ToSql() + $" WHERE {v}",
        OrderBy(var p, var v) => p.ToSql() + $" ORDER BY {v}",
        InnerJoin(var p, var v) => p.ToSql() + $" INNER JOIN {v}",
        LeftJoin(var p, var v) => p.ToSql() + $" LEFT JOIN {v}",
        RightJoin(var p, var v) => p.ToSql() + $" RIGHT JOIN {v}",
        FullOuterJoin(var p, var v) => p.ToSql() + $" FULL OUTER JOIN {v}",
        On(var p, var v) => p.ToSql() + $" ON {v}",
        GroupBy(var p, var v) => p.ToSql() + $" GROUP BY {v}",
        Having(var p, var v) => p.ToSql() + $" HAVING {v}",
        Union(var p) => p.ToSql() + $" UNION",
        _ => throw new NotImplementedException(nameof(self))
    };

    public static string ToPrettySql(this Clause self) => self switch
    {
        Select(null, { Length: 0 }) => "SELECT *",
        Select(null, var xs) => $"SELECT {string.Join(", ", xs)}",
        Select(var p, { Length: 0 }) => p.ToPrettySql() + "\nSELECT *",
        Select(var p, var xs) => p.ToPrettySql() + $"\nSELECT {string.Join(',', xs)}",
        Top(var p, var v) => p.ToPrettySql().AppendAfter("\nSELECT", $" TOP ({v})"),
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

    private static string AppendAfter(this string self, string after, string value) =>
        self.Replace(after, after + value);

    public static T? Get<T>(this IDataReader self, string name) where T : struct =>
        (T)self.GetValue(self.GetOrdinal(name));

    #endregion

    #region IDataReader, IDataRecord

    public static IEnumerable<IDataRecord> AsEnumerable(this IDataReader self)
    {
        while (self.Read())
            yield return self;
    }

    public static T GetValueAs<T>(this IDataRecord self, string columnName) =>
        (T)self.GetValue(self.GetOrdinal(columnName));

    public static T? GetValueAsNullable<T>(this IDataRecord self, string columnName) =>
        self.IsDBNull(self.GetOrdinal(columnName))
            ? default
            : (T?)self.GetValue(self.GetOrdinal(columnName));

    #endregion
}