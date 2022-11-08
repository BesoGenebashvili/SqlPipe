using System.Data;
using System.Data.SqlClient;
using System.Transactions;

Console.WriteLine("Hello, World!");

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

    public IList<TResult>? QueryList<TResult>(string sql, Func<IDataReader, TResult> map)
    {
        IList<TResult>? ListReader(IDataReader reader)
        {
            var xs = new List<TResult>();

            do xs.Add(map(reader));
            while (reader.Read());

            return xs;
        }

        return Query(sql, ListReader, CommandBehavior.Default);
    }

    public TResult? QuerySingle<TResult>(string sql, Func<IDataReader, TResult> map) =>
        Query(sql, map, CommandBehavior.SingleRow);

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